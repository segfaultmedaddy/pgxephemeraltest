package pgxephemeraltest

import (
	"cmp"
	"context"
	"crypto/rand"
	"fmt"
	"hash/fnv"
	"strconv"
	"strings"
	"time"

	"github.com/docker/docker/pkg/namesgenerator"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"go.segfaultmedaddy.com/pgxephemeraltest/internal/internaltesting"
)

const TemplatePrefix = "pgxephemeraltest_template_"

type factoryOptions struct {
	cleanupTimeout time.Duration
}

func (p *factoryOptions) defaults() { p.cleanupTimeout = DefaultCleanupTimeout }

// Migrator applies the migration to the database.
//
// Migrator is used to apply migrations for the template database
// on PoolFactory initialization, which is used for making copies of isolated
// ephemeral databases.
type Migrator interface {
	// Migrate applies migrations to the database.
	//
	// Migrate is typically run once during the PoolFactory instantiation
	// for template database initialization.
	Migrate(context.Context, *pgx.Conn) error

	// Hash returns a unique identifier for a given migration set.
	//
	// Each unique identifier is used to uniquely identify database template in
	// the target database.
	Hash() string
}

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
	//nolint:exhaustruct // defaults will initialize the missing fields.
	options := factoryOptions{}
	for _, opt := range opts {
		opt(&options)
	}

	options.defaults()

	//nolint:exhaustruct // init will initialize the missing fields.
	f := PoolFactory{
		cleanupTimeout: options.cleanupTimeout,
		config:         config.Copy(),
	}

	if err := f.init(ctx, migrator); err != nil {
		return nil, fmt.Errorf("pgxephemeraltest: failed to initialize factory: %w", err)
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

	mc, err := f.newMaintenanceConn(ctx)
	assertNoError(tb, err, "pgxephemeraltest: failed to get maintenance connection")

	db, err := randomName(6)
	assertNoError(tb, err, "pgxephemeraltest: failed to generate ephemeral database name")

	err = f.createDB(ctx, mc, db)
	assertNoError(tb, err, "pgxephemeraltest: failed to create ephemeral database")

	pool, err := f.pool(ctx, db)
	assertNoError(tb, err, "pgxephemeraltest: failed to connect to ephemeral database")

	tb.Logf("pgxephemeraltest: spun up a new ephemeral database for test: %s", db)

	tb.Cleanup(func() {
		pool.Close()

		ctx, cancel := context.WithTimeout(context.Background(), f.cleanupTimeout)
		defer cancel()

		defer mc.Close(ctx)

		// Leave the database intact if the test has failed for debugging
		if tb.Failed() {
			tb.Logf("pgxephemeraltest: failed test, leaving database intact: %s", db)

			return
		}

		err = f.dropDB(ctx, mc, db)
		if err != nil {
			tb.Logf("pgxephemeraltest: failed to drop ephemeral database: %s - %v", db, err)
		} else {
			tb.Logf("pgxephemeraltest: dropped ephemeral database: %s", db)
		}
	})

	return pool
}

// init creates a new template database owned by the supplied user.
func (f *PoolFactory) init(ctx context.Context, migrator Migrator) (err error) {
	var (
		user     = f.config.ConnConfig.User
		password = f.config.ConnConfig.Password
	)

	h := fnv.New64()
	h.Write([]byte(user))
	h.Write([]byte(password))
	h.Write([]byte(migrator.Hash()))

	template := TemplatePrefix + strconv.FormatUint(h.Sum64(), 10)
	f.template = template

	mc, err := f.newMaintenanceConn(ctx)
	if err != nil {
		return fmt.Errorf("pgxephemeraltest: failed to connect to database: %w", err)
	}
	defer mc.Close(ctx)

	// Linearize the creation of database template across multiple processes, since
	// it is a shared resource and can cause conflicts, when trying to initialize
	// it simultaneously.
	releaseLock, err := acquireLock(ctx, mc, template)
	if err != nil {
		return fmt.Errorf("pgxephemeraltest: failed to take lock: %w", err)
	}

	defer func() { err = releaseLock() }()

	if err := f.mkTemplate(ctx, migrator, user, template); err != nil {
		return fmt.Errorf("pgxephemeraltest: failed to create database template: %w", err)
	}

	return err
}

// newConn creates a new connection to the specified database.
// If db is empty, connects to the default maintenance database.
func (f *PoolFactory) newConn(ctx context.Context, db string) (*pgx.Conn, error) {
	connConfig := f.config.ConnConfig.Copy()
	connConfig.Database = cmp.Or(db, connConfig.Database)

	conn, err := pgx.ConnectConfig(ctx, connConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	if err := conn.Ping(ctx); err != nil {
		conn.Close(ctx)
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return conn, nil
}

// newMaintenanceConn creates a new maintenance connection.
//
// Maintenance connection is used to manage ephemeral databases lifecycle.
// The caller is responsible for closing the connection when done.
func (f *PoolFactory) newMaintenanceConn(ctx context.Context) (*pgx.Conn, error) {
	conn, err := f.newConn(ctx, "")
	if err != nil {
		return nil, fmt.Errorf("failed to acquire maintenance connection: %w", err)
	}

	return conn, nil
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

// mkTemplate creates a new database template with migrations applied.
// If the template exists, it will skip migration.
//
// Generally, mkTemplate is expected to be called only once at the factory
// initialization.
//
// mkTemplate is not thread-safe; attempting to run it concurrently will result in
// connection lock (pgx busy conn).
func (f *PoolFactory) mkTemplate(ctx context.Context, migrator Migrator, user, template string) error {
	mc, err := f.newMaintenanceConn(ctx)
	if err != nil {
		return fmt.Errorf("pgxephemeraltest: failed to get maintenance connection: %w", err)
	}
	defer mc.Close(ctx)

	// It is safe to check only if the database is marked as a template, since marking
	// it as a template is the last step in the process.
	var doesTemplateExists bool
	if err := mc.QueryRow(ctx, "SELECT exists(SELECT 1 FROM pg_database WHERE datname = $1 AND datistemplate = true)",
		template).Scan(&doesTemplateExists); err != nil {
		return fmt.Errorf("pgxephemeraltest: failed to check if template exists: %w", err)
	}

	if doesTemplateExists {
		return nil // Template already exists
	}

	// If template doesn't exist, it could fail at marking it as a template, yet
	// succeed at creating it. Let's try to clean it up.
	if _, err := mc.Exec(ctx, strings.Join([]string{
		"DROP DATABASE IF EXISTS",
		pgx.Identifier{template}.Sanitize(),
	}, " ")); err != nil {
		return fmt.Errorf("pgxephemeraltest: failed to drop existing database template: %w", err)
	}

	if _, err := mc.Exec(ctx, strings.Join([]string{
		"CREATE DATABASE",
		pgx.Identifier{template}.Sanitize(),
		"OWNER",
		pgx.Identifier{user}.Sanitize(),
	}, " ")); err != nil {
		return fmt.Errorf("pgxephemeraltest: failed to create database template: %w", err)
	}

	// Connect to the template database to run migrations in the newly created database.
	tc, err := f.newConn(ctx, template)
	if err != nil {
		return fmt.Errorf("pgxephemeraltest: failed to connect to template database: %w", err)
	}
	defer tc.Close(ctx)

	if err := migrator.Migrate(ctx, tc); err != nil {
		return fmt.Errorf("pgxephemeraltest: failed to run migrations: %w", err)
	}

	if _, err := mc.Exec(ctx, "UPDATE pg_database SET datistemplate = true WHERE datname = $1", template); err != nil {
		return fmt.Errorf("pgxephemeraltest: failed to finalize database template: %w", err)
	}

	return nil
}

// createDB creates a new db ephemeral database for testing.
func (f *PoolFactory) createDB(ctx context.Context, mc *pgx.Conn, db string) error {
	_, err := mc.Exec(ctx, strings.Join([]string{
		"CREATE DATABASE",
		pgx.Identifier{db}.Sanitize(),
		"TEMPLATE",
		pgx.Identifier{f.template}.Sanitize(),
		"OWNER",
		pgx.Identifier{f.config.ConnConfig.User}.Sanitize(),
	}, " "))
	if err != nil {
		return fmt.Errorf("pgxephemeraltest: failed to copy database template: %w", err)
	}

	return nil
}

// dropDB drops the db ephemeral database after testing.
func (f *PoolFactory) dropDB(ctx context.Context, mc *pgx.Conn, db string) error {
	_, err := mc.Exec(ctx, strings.Join([]string{"DROP DATABASE", pgx.Identifier{db}.Sanitize()}, " "))
	if err != nil {
		return fmt.Errorf("pgxephemeraltest: failed to drop database %s: %w", db, err)
	}

	return nil
}

// acquireLock acquires a postgres advisory lock.
//
// The caller is responsible for releasing the lock by calling the returned function.
func acquireLock(ctx context.Context, conn *pgx.Conn, name string) (func() error, error) {
	h := fnv.New32()
	h.Write([]byte(name))
	lockNum := int64(h.Sum32())

	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock($1::BIGINT)", lockNum); err != nil {
		return nil, fmt.Errorf("pgxephemeraltest: failed to acquire lock: %w", err)
	}

	return func() error {
		if _, err := conn.Exec(ctx, "SELECT pg_advisory_unlock($1::BIGINT)", lockNum); err != nil {
			return fmt.Errorf("pgxephemeraltest: failed to release lock: %w", err)
		}

		return nil
	}, nil
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
