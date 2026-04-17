package pgxephemeraltest

import (
	"context"
	"crypto/rand"
	"fmt"
	"time"

	"github.com/docker/docker/pkg/namesgenerator"
	"github.com/jackc/pgx/v5/pgxpool"

	"go.segfaultmedaddy.com/pgxephemeraltest/internal/dbmanager"
	"go.segfaultmedaddy.com/pgxephemeraltest/internal/internaltesting"
)

type factoryOptions struct {
	cleanupTimeout time.Duration
}

func (p *factoryOptions) defaults() { p.cleanupTimeout = DefaultCleanupTimeout }

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
	m              *dbmanager.DBManager
	config         *pgxpool.Config
	template       string
	cleanupTimeout time.Duration
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
	var options factoryOptions
	for _, opt := range opts {
		opt(&options)
	}

	options.defaults()

	m, err := dbmanager.New(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("pgxephemeraltest: failed to initialize factory: %w", err)
	}

	template := dbmanager.TemplateName(config.ConnConfig, migrator)

	if err := m.Init(ctx, migrator, template); err != nil {
		return nil, fmt.Errorf("pgxephemeraltest: failed to initialize factory: %w", err)
	}

	f := PoolFactory{
		cleanupTimeout: options.cleanupTimeout,
		config:         config.Copy(),
		m:              m,
		template:       template,
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
// the test is done. If a test is failed the database is left intact for debugging,
// otherwise it is dropped.
func (f *PoolFactory) Pool(tb internaltesting.TB) *pgxpool.Pool {
	tb.Helper()

	ctx := tb.Context()

	db, err := f.createDB(ctx)
	assertNoError(tb, err, "pgxephemeraltest: failed to create ephemeral database")

	pool, err := f.pool(ctx, db)
	assertNoError(tb, err, "pgxephemeraltest: failed to create ephemeral database")

	tb.Logf("pgxephemeraltest: spun up a new ephemeral database for test: %s", db)

	tb.Cleanup(func() {
		pool.Close()

		ctx, cancel := context.WithTimeout(context.Background(), f.cleanupTimeout)
		defer cancel()

		// Leave the database intact if the test has failed for debugging
		if tb.Failed() {
			tb.Logf("pgxephemeraltest: failed test, leaving database intact: %s", db)

			return
		}

		if err := f.m.DropDB(ctx, db, false); err != nil {
			tb.Logf("pgxephemeraltest: failed to drop ephemeral database: %s - %v", db, err)
		} else {
			tb.Logf("pgxephemeraltest: dropped ephemeral database: %s", db)
		}
	})

	return pool
}

func (f *PoolFactory) createDB(ctx context.Context) (string, error) {
	db, err := randomName(6)
	if err != nil {
		return "", err
	}

	db, err = f.m.CreateDB(ctx, f.template, db)
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

// randomName generates a random name using docker names alphabet
// and random suffix.
//
// n is the length of the random suffix.
func randomName(n int) (string, error) {
	name := namesgenerator.GetRandomName(0)

	bytes := make([]byte, n/2)
	if _, err := rand.Read(bytes); err != nil {
		return "", fmt.Errorf("pgxephemeraltest: failed to generate random name: %w", err)
	}

	return fmt.Sprintf("%s_%x", name, bytes), nil
}
