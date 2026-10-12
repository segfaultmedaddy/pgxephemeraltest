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
	lazyInitialization          bool
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
	m             *dbmanager.DBManager
	config        *pgxpool.Config
	names         *namegenerator.Generator
	migrator      Migrator
	parent        *PoolFactory
	initLockCh    chan struct{}
	template      string
	options       factoryOptions
	isInitialized bool
}

// NewPoolFactory creates a new PoolFactory instance.
//
// It initializes a new database, applies migration to it and marks it as
// a template. The template is copied for each newly created ephemeral database.
// WithLazyInitialization(true) defers database initialization until the first
// Pool call. The supplied context is only used for eager initialization.
func NewPoolFactory(
	ctx context.Context,
	config *pgxpool.Config,
	migrator Migrator,
	opts ...FactoryOption,
) (*PoolFactory, error) {
	return newPoolFactory(ctx, config, migrator, nil, opts...)
}

func newPoolFactory(
	ctx context.Context,
	config *pgxpool.Config,
	migrator Migrator,
	parent *PoolFactory,
	opts ...FactoryOption,
) (*PoolFactory, error) {
	var options factoryOptions
	if parent != nil {
		options = parent.options
	} else {
		options = factoryOptions{
			cleanupTimeout:              DefaultCleanupTimeout,
			shouldKeepDatabaseOnFailure: true,
			lazyInitialization:          false,
		}
	}

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
	if parent == nil {
		template = dbmanager.TemplateName(config.ConnConfig, migrator)
	} else {
		template = dbmanager.DerivedTemplateName(parent.template, migrator)
	}

	f := PoolFactory{
		config:        config.Copy(),
		m:             m,
		template:      template,
		names:         names,
		migrator:      migrator,
		parent:        parent,
		initLockCh:    make(chan struct{}, 1),
		options:       options,
		isInitialized: false,
	}

	if !options.lazyInitialization {
		if err := f.init(ctx); err != nil {
			return nil, err
		}
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
// Lazy initialization is inherited too: a lazy child does not initialize either
// template until Pool is called. An eager child initializes its parent
// first, even if the parent is lazy.
func (f *PoolFactory) Derive(
	ctx context.Context,
	migrator Migrator,
	opts ...FactoryOption,
) (*PoolFactory, error) {
	child, err := newPoolFactory(ctx, f.config, migrator, f, opts...)
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
// for the lifetime of the factory. It does not initialize a lazy factory.
func (f *PoolFactory) Template() string { return f.template }

// Pool returns a pool connected to a newly created isolated database
// ready for use.
//
// It is expected that each test case uses a separate database, which
// helps to isolate tests and prevent data leakage between them.
// Lazy templates and their ancestors are initialized using the test's context.
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

// init initializes the factory's template, initializing any parent templates
// first. Pool calls it automatically using the test's context.
//
// Concurrent calls are safe and wait for initialization using their own contexts.
// Successful initialization is remembered; failed or canceled attempts can be
// retried. The constructor's context is not retained.
func (f *PoolFactory) init(ctx context.Context) error {
	select {
	case f.initLockCh <- struct{}{}:
		defer func() { <-f.initLockCh }()
	case <-ctx.Done():
		return fmt.Errorf("pgxephemeraltest: failed to initialize factory: %w", ctx.Err())
	}

	if f.isInitialized {
		return nil
	}

	if err := ctx.Err(); err != nil {
		return fmt.Errorf("pgxephemeraltest: failed to initialize factory: %w", err)
	}

	var source string

	if f.parent != nil {
		if err := f.parent.init(ctx); err != nil {
			return fmt.Errorf(
				"pgxephemeraltest: failed to initialize parent template %q: %w",
				f.parent.template,
				err,
			)
		}

		source = f.parent.template
	}

	if err := f.m.InitFrom(ctx, f.migrator, f.template, source); err != nil {
		return fmt.Errorf("pgxephemeraltest: failed to initialize factory: %w", err)
	}

	f.isInitialized = true

	return nil
}

func (f *PoolFactory) createDB(ctx context.Context, testName string) (string, error) {
	if err := f.init(ctx); err != nil {
		return "", err
	}

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
