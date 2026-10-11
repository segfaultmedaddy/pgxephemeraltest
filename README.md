# pgxephemeraltest

Isolated database testing for Go using ephemeral transactions and databases.

pgxephemeraltest provides isolated test environments for database-backed functionality in Go. It exposes wrappers around two common testing patterns:

- **Ephemeral transactions**: Tests run in transactions that are rolled back after completion
- **Ephemeral databases**: Tests run in fully isolated databases created from templates

When using ephemeral databases, the package leverages PostgreSQL [templates](https://www.postgresql.org/docs/current/manage-ag-templatedbs.html) for a quick initialization of a new database.

## Usage

```sh
$ go get go.segfaultmedaddy.com/pgxephemeraltest/v2
```

### 1. Define your schema and seed migrators

```go
type schemaMigrator struct{}

func (schemaMigrator) Migrate(ctx context.Context, conn *pgx.Conn) error {
    _, err := conn.Exec(ctx, `
        CREATE TABLE users (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL
        )
    `)
    return err
}

func (schemaMigrator) Hash() string { return "users-schema-v1" }

type seedMigrator struct{}

func (seedMigrator) Migrate(ctx context.Context, conn *pgx.Conn) error {
    _, err := conn.Exec(ctx, `INSERT INTO users (name) VALUES ($1)`, "Alice")
    return err
}

func (seedMigrator) Hash() string { return "users-seed-v1" }
```

The schema migrator creates the `users` table. The seed migrator inserts fixtures into the inherited table without recreating it. Change each migrator's `Hash()` when its schema or seed data changes.

### 2. Initialize the base and derived factories

```go
var (
    schemaFactory *pgxephemeraltest.PoolFactory
    seededFactory *pgxephemeraltest.PoolFactory
)

func TestMain(m *testing.M) {
    ctx := context.Background()

    connString, ok := os.LookupEnv("TEST_DATABASE_URL")
    if !ok {
        panic("TEST_DATABASE_URL environment variable not set")
    }

    var err error
    schemaFactory, err = pgxephemeraltest.NewPoolFactoryFromConnString(ctx, connString, schemaMigrator{})
    if err != nil {
        panic(err)
    }

    seededFactory, err = schemaFactory.Derive(ctx, seedMigrator{})
    if err != nil {
        panic(err)
    }

    os.Exit(m.Run())
}
```

`schemaFactory` provides an empty `users` table; `seededFactory` provides the same schema with Alice already inserted. Derivation clones the schema template and runs only `seedMigrator`, leaving the base template unchanged. Each pool from the derived factory starts with its own copy of the seed data.

By default, databases from failed tests are kept for debugging. Pass `pgxephemeraltest.WithKeepDatabaseOnFailure(false)` to `NewPoolFactoryFromConnString` or `Derive` to drop them even on failure. Derived factories inherit their parent's cleanup timeout and failure-retention options; options passed to `Derive` override inherited values without changing the parent.

### 3. Write isolated tests

```go
func TestUsers(t *testing.T) {
    t.Parallel()

    ctx := context.Background()
    pool := schemaFactory.Pool(t) // an isolated database with an empty users table

    _, err := pool.Exec(ctx, `INSERT INTO users (name) VALUES ($1)`, "Bob")
    require.NoError(t, err)

    var name string
    err = pool.QueryRow(ctx, `SELECT name FROM users WHERE name = $1`, "Bob").Scan(&name)
    require.NoError(t, err)
    require.Equal(t, "Bob", name)
}

func TestSeededUsers(t *testing.T) {
    t.Parallel()

    ctx := context.Background()
    pool := seededFactory.Pool(t) // an isolated database with Alice already seeded

    var name string
    err := pool.QueryRow(ctx, `SELECT name FROM users WHERE name = $1`, "Alice").Scan(&name)
    require.NoError(t, err)
    require.Equal(t, "Alice", name)
}
```

Every `Pool(t)` call on either factory creates a new database and pool, even for the same test. Reuse the returned pool to share state within a test.

For more usage examples, check out the `examples` directory in the root of this repository.

## Filesystem migrations

Use `pkg/migrator` to build a migrator from SQL files on disk or an embedded filesystem:

```go
import "go.segfaultmedaddy.com/pgxephemeraltest/v2/pkg/migrator"

m, err := migrator.FromFS(os.DirFS("migrations"))
if err != nil {
    panic(err)
}

factory, err := pgxephemeraltest.NewPoolFactoryFromConnString(ctx, connString, m)
```

`FromFS` recursively loads regular `.sql` files and executes them in lexicographic order of their full paths. Statements within each file are executed in order, and migration stops at the first error. Use `fs.Sub` to select a subdirectory of an embedded filesystem, or `migrator.FromFile(fsys, "schema.sql")` to load a single file.

The migrator snapshots file contents when constructed. Its hash includes the ordered filenames and contents, so changing the migration set produces a different database template.

## How It Works

Both approaches provide isolation, with different trade-offs:

- Transactions are faster but share the same database
- Separate databases provide complete isolation but are slightly slower

For a deeper dive, check out [segfaultmedaddy.com/p/pgxephemeraltest](https://segfaultmedaddy.com/p/pgxephemeraltest) for detailed information on the design and usage of this package.

> The package is deliberately built on top of the [pgx](https://github.com/jackc/pgx) driver and does not support the standard `database/sql` interface.
> If you need support for the standard `database/sql` interface, consider using [pgtestdb](https://github.com/peterldowns/pgtestdb).

## License

MIT Licensed
