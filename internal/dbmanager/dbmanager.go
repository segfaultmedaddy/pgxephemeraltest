package dbmanager

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"slices"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	TemplatePrefix = "pgxephemeraltest_template_"
	DatabasePrefix = "pgxephemeraltest_db_"
)

// Migrator applies the migration to the database.
//
// Migrator is used to apply migrations for the template database
// on db manager initialization, which is used for making copies of isolated
// ephemeral databases.
type Migrator interface {
	// Migrate applies migrations to the database.
	//
	// Migrate is typically run once during the db manager instantiation
	// for template database initialization.
	Migrate(context.Context, *pgx.Conn) error

	// Hash returns a unique identifier for a given migration set.
	//
	// Each unique identifier is used to uniquely identify database template in
	// the target database.
	Hash() string
}

// DBInfo holds information about a database.
type DBInfo struct {
	// Name is the name of the database.
	Name string `json:"name"`

	// IsTemplate indicates whether the database is a template database.
	IsTemplate bool `json:"isTemplate"`
}

// DBManager manages lifecycle of a set of ephemeral databases
// used for testing purposes.
//
// It helps to create a completely new database for each test allowing
// to run them in parallel without interfering with each other avoiding data
// leakage between tests.
//
// Each created database is prepared with applied migration provided by running
// provided migrator.
type DBManager struct {
	config *pgxpool.Config
}

// New creates a new DBManager instance.
//
// It initializes a new database, applies migration to it and marks it as
// a template. The template is copied for each newly created ephemeral database.
func New(
	_ context.Context,
	config *pgxpool.Config,
) (*DBManager, error) {
	m := DBManager{config: config.Copy()}

	return &m, nil
}

// Init creates a new template database owned by the supplied user.
func (f *DBManager) Init(ctx context.Context, migrator Migrator, tpl string) (err error) {
	mc, err := f.newMaintenanceConn(ctx)
	if err != nil {
		return fmt.Errorf("pgxephemeraltest: failed to connect to database: %w", err)
	}
	defer mc.Close(ctx)

	// Linearize the creation of database template across multiple processes, since
	// it is a shared resource and can cause conflicts, when trying to initialize
	// it simultaneously.
	releaseLock, err := acquireLock(ctx, mc, tpl)
	if err != nil {
		return fmt.Errorf("pgxephemeraltest: failed to take lock: %w", err)
	}

	defer func() {
		releaseErr := releaseLock(context.WithoutCancel(ctx))
		if releaseErr == nil {
			return
		}

		if err == nil {
			err = releaseErr
			return
		}

		err = errors.Join(err, releaseErr)
	}()

	if err := f.mkTemplate(ctx, migrator, f.config.ConnConfig.User, tpl); err != nil {
		return fmt.Errorf("pgxephemeraltest: failed to create database template: %w", err)
	}

	return err
}

// CreateDB creates a new db ephemeral database and returns the database name.
func (f *DBManager) CreateDB(ctx context.Context, tpl string, db string) (string, error) {
	mc, err := f.newMaintenanceConn(ctx)
	if err != nil {
		return "", fmt.Errorf("pgxephemeraltest: failed to acquire maintenance connection: %w", err)
	}
	defer mc.Close(ctx)

	db = DatabasePrefix + db

	_, err = mc.Exec(ctx, strings.Join([]string{
		"CREATE DATABASE",
		pgx.Identifier{db}.Sanitize(),
		"TEMPLATE",
		pgx.Identifier{tpl}.Sanitize(),
		"OWNER",
		pgx.Identifier{f.config.ConnConfig.User}.Sanitize(),
	}, " "))
	if err != nil {
		return "", fmt.Errorf("pgxephemeraltest: failed to copy database template: %w", err)
	}

	return db, nil
}

// DropDB drops the db ephemeral database after testing.
func (f *DBManager) DropDB(ctx context.Context, db string) error {
	if !strings.HasPrefix(db, DatabasePrefix) && !strings.HasPrefix(db, TemplatePrefix) {
		return fmt.Errorf("pgxephemeraltest: refusing to drop unmanaged database %q", db)
	}

	mc, err := f.newMaintenanceConn(ctx)
	if err != nil {
		return fmt.Errorf("pgxephemeraltest: failed to acquire maintenance connection: %w", err)
	}
	defer mc.Close(ctx)

	if _, err := mc.Exec(
		ctx,
		"UPDATE pg_database SET datistemplate = false WHERE datname = $1 AND datistemplate = true",
		db,
	); err != nil {
		return fmt.Errorf("pgxephemeraltest: failed to unset template flag: %w", err)
	}

	if _, err := mc.Exec(
		ctx,
		strings.Join([]string{"DROP DATABASE", pgx.Identifier{db}.Sanitize()}, " "),
	); err != nil {
		return fmt.Errorf("pgxephemeraltest: failed to drop database %s: %w", db, err)
	}

	return nil
}

func (f *DBManager) DropDBs(ctx context.Context, dbs []string) error {
	if len(dbs) == 0 {
		return nil
	}

	for _, dbName := range dbs {
		if strings.HasPrefix(dbName, DatabasePrefix) || strings.HasPrefix(dbName, TemplatePrefix) {
			continue
		}

		return fmt.Errorf("pgxephemeraltest: refusing to drop unmanaged database %q", dbName)
	}

	listedKnownDBs, err := f.ListDBs(ctx)
	if err != nil {
		return fmt.Errorf("pgxephemeraltest: failed to verify databases before drop: %w", err)
	}

	availableDBs := make(map[string]DBInfo, len(listedKnownDBs))
	for _, db := range listedKnownDBs {
		availableDBs[db.Name] = db
	}

	templates := make([]string, 0, len(dbs))
	for _, dbName := range dbs {
		info, ok := availableDBs[dbName]
		if !ok {
			return fmt.Errorf("pgxephemeraltest: database %q not found", dbName)
		}

		if info.IsTemplate {
			templates = append(templates, dbName)
		}
	}

	mc, err := f.newMaintenanceConn(ctx)
	if err != nil {
		return fmt.Errorf("pgxephemeraltest: failed to acquire maintenance connection: %w", err)
	}
	defer mc.Close(ctx)

	if len(templates) > 0 {
		for chunk := range slices.Chunk(templates, 256) {
			if _, err := mc.Exec(
				ctx,
				"UPDATE pg_database SET datistemplate = false WHERE datname = ANY($1)",
				chunk,
			); err != nil {
				return fmt.Errorf("pgxephemeraltest: failed to unset template flag: %w", err)
			}
		}
	}

	for chunk := range slices.Chunk(dbs, 256) {
		var b pgx.Batch
		for _, dbName := range chunk {
			b.Queue(strings.Join([]string{"DROP DATABASE", pgx.Identifier{dbName}.Sanitize()}, " "))
		}

		result := mc.SendBatch(ctx, &b)
		if err := result.Close(); err != nil {
			return fmt.Errorf("pgxephemeraltest: failed to close batch results: %w", err)
		}
	}

	return nil
}

func (f *DBManager) ListDBs(ctx context.Context) ([]DBInfo, error) {
	mc, err := f.newMaintenanceConn(ctx)
	if err != nil {
		return nil, fmt.Errorf("pgxephemeraltest: failed to acquire maintenance connection: %w", err)
	}
	defer mc.Close(ctx)

	rows, err := mc.Query(
		ctx,
		"SELECT datname, datistemplate FROM pg_database WHERE datname LIKE $1 OR datname LIKE $2 ORDER BY oid",
		TemplatePrefix+"%",
		DatabasePrefix+"%",
	)
	if err != nil {
		return nil, fmt.Errorf("pgxephemeraltest: failed to list databases: %w", err)
	}
	defer rows.Close()

	var dbs []DBInfo

	for rows.Next() {
		var db DBInfo
		if err := rows.Scan(&db.Name, &db.IsTemplate); err != nil {
			return nil, fmt.Errorf("pgxephemeraltest: failed to scan database: %w", err)
		}

		dbs = append(dbs, db)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("pgxephemeraltest: failed while iterating databases: %w", err)
	}

	return dbs, nil
}

// newConn creates a new connection to the specified database.
// If db is empty, connects to the default maintenance database.
func (f *DBManager) newConn(ctx context.Context, db string) (*pgx.Conn, error) {
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
func (f *DBManager) newMaintenanceConn(ctx context.Context) (*pgx.Conn, error) {
	conn, err := f.newConn(ctx, "")
	if err != nil {
		return nil, fmt.Errorf("failed to acquire maintenance connection: %w", err)
	}

	return conn, nil
}

// mkTemplate creates a new database template with migrations applied.
// If the template exists, it will skip migration.
//
// Generally, mkTemplate is expected to be called only once at the factory
// initialization.
//
// mkTemplate is not thread-safe; attempting to run it concurrently will result in
// connection lock (pgx busy conn).
func (f *DBManager) mkTemplate(ctx context.Context, migrator Migrator, user, template string) error {
	mc, err := f.newMaintenanceConn(ctx)
	if err != nil {
		return fmt.Errorf("pgxephemeraltest: failed to get maintenance connection: %w", err)
	}
	defer mc.Close(ctx)

	// It is safe to check only if the database is marked as a template, since marking
	// it as a template is the last step in the process.
	var doesTemplateExists bool
	if err := mc.QueryRow(ctx, "SELECT exists(SELECT 1 FROM pg_database WHERE datname = $1 AND datistemplate = true)",
		template).
		Scan(&doesTemplateExists); err != nil {
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

	if _, err := mc.Exec(
		ctx,
		"UPDATE pg_database SET datistemplate = true WHERE datname = $1",
		template,
	); err != nil {
		return fmt.Errorf("pgxephemeraltest: failed to finalize database template: %w", err)
	}

	return nil
}

// acquireLock acquires a postgres advisory lock.
//
// The caller is responsible for releasing the lock by calling the returned function.
func acquireLock(ctx context.Context, conn *pgx.Conn, name string) (func(ctx context.Context) error, error) {
	h := fnv.New32()
	h.Write([]byte(name))
	lockNum := int64(h.Sum32())

	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock($1::BIGINT)", lockNum); err != nil {
		return nil, fmt.Errorf("pgxephemeraltest: failed to acquire lock: %w", err)
	}

	return func(ctx context.Context) error {
		if _, err := conn.Exec(ctx, "SELECT pg_advisory_unlock($1::BIGINT)", lockNum); err != nil {
			return fmt.Errorf("pgxephemeraltest: failed to release lock: %w", err)
		}

		return nil
	}, nil
}

// TemplateName returns a unique template name for the given migration set.
func TemplateName(config *pgx.ConnConfig, m Migrator) string {
	h := fnv.New64()
	h.Write([]byte(config.User))
	h.Write([]byte(config.Password))
	h.Write([]byte(m.Hash()))

	template := TemplatePrefix + strconv.FormatUint(h.Sum64(), 16)

	return template
}
