package pgxephemeraltest

import (
	"cmp"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"go.segfaultmedaddy.com/pgxephemeraltest/v2/internal/dbmanager"
	"go.segfaultmedaddy.com/pgxephemeraltest/v2/internal/internaltesting"
	"go.segfaultmedaddy.com/pgxephemeraltest/v2/internal/namegenerator"
)

type factoryOptions struct {
	cleanupTimeout              time.Duration
	shouldKeepDatabaseOnFailure bool
}

func (p *factoryOptions) defaults() {
	p.cleanupTimeout = cmp.Or(p.cleanupTimeout, DefaultCleanupTimeout)
}

const (
	DatabasePrefix = dbmanager.DatabasePrefix
	TemplatePrefix = dbmanager.TemplatePrefix
)

// Migrator applies the migration to the database.
//
// Migrator is used to apply migrations for the template database
// on PoolFactory initialization, which is used for making copies of isolated
// ephemeral databases.
type Migrator = dbmanager.Migrator

// PoolFactory manages lifecycle of a set of ephemeral databases
// used for testing purposes.
//
// It helps to create a completely new database for each test allowing
// to run them in parallel without interfering with each other avoiding data
// leakage between tests.
//
// Each created database is prepared with applied migration provided by running
// provided migrator.
type PoolFactory struct {
	m        *dbmanager.DBManager
	config   *pgxpool.Config
	names    *namegenerator.Generator
	template string
	options  factoryOptions
}

// NewPoolFactory creates a new PoolFactory instance.
//
// It initializes a new database, applies migration to it and marks it as
// a template. The template is copied for each newly created ephemeral database.
func NewPoolFactory(
	ctx context.Context,
	config *pgxpool.Config,
	migrator Migrator,
	opts ...FactoryOption,
) (*PoolFactory, error) {
	return newPoolFactory(ctx, config, migrator, "", factoryOptions{
		cleanupTimeout:              DefaultCleanupTimeout,
		shouldKeepDatabaseOnFailure: true,
	}, opts...)
}

func newPoolFactory(
	ctx context.Context,
	config *pgxpool.Config,
	migrator Migrator,
	parent string,
	options factoryOptions,
	opts ...FactoryOption,
) (*PoolFactory, error) {
	for _, opt := range opts {
		opt(&options)
	}

	options.defaults()

	names, err := namegenerator.New(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("pgxephemeraltest: failed to initialize factory: %w", err)
	}

	m, err := dbmanager.New(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("pgxephemeraltest: failed to initialize factory: %w", err)
	}

	var template string
	if parent == "" {
		template = dbmanager.TemplateName(config.ConnConfig, migrator)
	} else {
		template = dbmanager.DerivedTemplateName(parent, migrator)
	}

	if err := m.InitFrom(ctx, migrator, template, parent); err != nil {
		return nil, fmt.Errorf("pgxephemeraltest: failed to initialize factory: %w", err)
	}

	f := PoolFactory{
		config:   config.Copy(),
		m:        m,
		template: template,
		names:    names,
		options:  options,
	}

	return &f, nil
}

// NewPoolFactoryFromConnString is like NewPoolFactory, but the base pool config
// is provided via connection string.
func NewPoolFactoryFromConnString(
	ctx context.Context,
	connString string,
	migrator Migrator,
	opts ...FactoryOption,
) (*PoolFactory, error) {
	config, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, fmt.Errorf("pgxephemeraltest: failed to parse connection string: %w", err)
	}

	return NewPoolFactory(ctx, config, migrator, opts...)
}

// Derive creates a factory whose template is cloned from f's template and then
// migrated using only the supplied migrator. The parent template is not modified.
// A derived factory can itself be used as a parent for further derivation.
//
// The template is cached by the parent template name and the migrator's Hash.
// Concurrent calls for the same template are serialized by a PostgreSQL advisory
// lock; different child templates can be initialized in parallel, even when they
// share a parent. A migrator shared across different templates must support
// concurrent calls.
//
// The child inherits a copy of f's connection configuration, including its
// maintenance database, and its factory options. Options supplied in opts override
// the inherited values for the child without changing the parent or its siblings.
func (f *PoolFactory) Derive(
	ctx context.Context,
	migrator Migrator,
	opts ...FactoryOption,
) (*PoolFactory, error) {
	child, err := newPoolFactory(ctx, f.config, migrator, f.template, f.options, opts...)
	if err != nil {
		return nil, fmt.Errorf(
			"pgxephemeraltest: failed to derive factory from template %q: %w",
			f.template,
			err,
		)
	}

	return child, nil
}

// Template returns the template name used to create ephemeral databases.
//
// It is computed once on PoolFactory creation and stays the same
// for the lifetime of the factory.
func (f *PoolFactory) Template() string { return f.template }

// Pool returns a pool connected to a newly created isolated database
// ready for use.
//
// It is expected that each test case uses a separate database, which
// helps to isolate tests and prevent data leakage between them.
//
// Lifetime of the pool is managed by the tb, the pool is closed when
// the test is done. By default, a failed test's database is left intact for debugging;
// otherwise it is dropped. Use WithKeepDatabaseOnFailure(false) to drop it on failure too.
func (f *PoolFactory) Pool(tb internaltesting.TB) *pgxpool.Pool {
	tb.Helper()

	ctx := tb.Context()

	db, err := f.createDB(ctx, tb.Name())
	assertNoError(tb, err, "pgxephemeraltest: failed to create ephemeral database")

	pool, err := f.pool(ctx, db)
	if err != nil {
		ctx, cancel := context.WithTimeout(context.Background(), f.options.cleanupTimeout)
		defer cancel()

		if dropErr := f.m.DropDB(ctx, db); dropErr != nil {
			err = errors.Join(
				err,
				fmt.Errorf(
					"pgxephemeraltest: failed to cleanup ephemeral database after connection failure: %w",
					dropErr,
				),
			)
		}

		assertNoError(tb, err, "pgxephemeraltest: failed to connect to ephemeral database")
	}

	tb.Logf("pgxephemeraltest: spun up a new ephemeral database for test: %s", db)

	tb.Cleanup(func() {
		pool.Close()

		ctx, cancel := context.WithTimeout(context.Background(), f.options.cleanupTimeout)
		defer cancel()

		// Leave the database intact if the test has failed for debugging.
		if tb.Failed() && f.options.shouldKeepDatabaseOnFailure {
			tb.Logf("pgxephemeraltest: failed test, leaving database intact: %s", db)

			return
		}

		if err := f.m.DropDB(ctx, db); err != nil {
			tb.Errorf("pgxephemeraltest: failed to drop ephemeral database: %s - %v", db, err)
		} else {
			tb.Logf("pgxephemeraltest: dropped ephemeral database: %s", db)
		}
	})

	return pool
}

func (f *PoolFactory) createDB(ctx context.Context, testName string) (string, error) {
	db, err := f.m.CreateDB(ctx, f.template, f.names.Generate(testName))
	if err != nil {
		return "", fmt.Errorf("create ephemeral database from template %q: %w", f.template, err)
	}

	return db, nil
}

// pool creates a new pool connected to the db ephemeral database.
func (f *PoolFactory) pool(ctx context.Context, db string) (*pgxpool.Pool, error) {
	config := f.config.Copy()
	config.ConnConfig.Database = db

	p, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("pgxephemeraltest: failed to create pool: %w", err)
	}

	if err := p.Ping(ctx); err != nil {
		p.Close()
		return nil, fmt.Errorf("pgxephemeraltest: failed to ping database: %w", err)
	}

	return p, nil
}
