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

### 1. Define your migrator

```go
type migrator struct{}

func (migrator) Migrate(ctx context.Context, conn *pgx.Conn) error {
    _, err := conn.Exec(ctx, `
        CREATE TABLE users (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL
        )
    `)
    return err
}

func (migrator) Hash() string { return "v1" }
```

### 2. Initialize the factory

```go
var factory *pgxephemeraltest.PoolFactory

func TestMain(m *testing.M) {
    ctx := context.Background()

    connString, ok := os.LookupEnv("TEST_DATABASE_URL")
    if !ok {
        panic("TEST_DATABASE_URL environment variable not set")
    }

    var err error
    factory, err = pgxephemeraltest.NewPoolFactoryFromConnString(ctx, connString, &migrator{})
    if err != nil {
        panic(err)
    }

    os.Exit(m.Run())
}
```

By default, databases from failed tests are kept for debugging. Pass `pgxephemeraltest.WithKeepDatabaseOnFailure(false)` to `NewPoolFactoryFromConnString` to drop them even on failure.

### 3. Write isolated tests

```go
func TestUsers(t *testing.T) {
    t.Parallel()

    ctx := context.Background()
    pool := factory.Pool(t) // a pool connected to a newly created fully isolated database

    _, err := pool.Exec(ctx, `INSERT INTO users (name) VALUES ($1)`, "Alice")
    require.NoError(t, err)

    var name string
    err = pool.QueryRow(ctx, `SELECT name FROM users WHERE name = $1`, "Alice").Scan(&name)
    require.NoError(t, err)
    require.Equal(t, "Alice", name)
}
```

Every `factory.Pool(t)` call creates a new database and pool, even for the same test. Reuse the returned pool to share state within a test.

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

## Derived templates

Use `Derive` to clone an initialized factory's template and apply a different migrator on top:

```go
parent, err := pgxephemeraltest.NewPoolFactoryFromConnString(ctx, connString, baseMigrator)
if err != nil {
    panic(err)
}

child, err := parent.Derive(ctx, featureMigrator,
    pgxephemeraltest.WithKeepDatabaseOnFailure(false),
)
if err != nil {
    panic(err)
}

pool := child.Pool(t) // contains the base schema/data plus the feature migration
```

Only the child migrator runs during derivation; the parent is not modified or migrated again. The child migrator may be a different implementation, including `pkg/migrator.FSMigrator`. A derived factory can itself be a parent for further derivation.

Child templates are cached by the parent template's identity and the child migrator's `Hash()`. Changing a parent migration therefore changes descendant template identities, even if their local migrations remain the same. `Hash()` should identify only the local migration. Child migrators must account for inherited schema, data, and migration-history tables, and must commit their changes and close any additional database connections before returning.

`Derive` is safe to call concurrently. Creation of the same child template is serialized using a PostgreSQL advisory lock, including across processes. Different child templates can be cloned and migrated concurrently, even when they share a parent. A migrator instance reused across different templates must support concurrent calls.

The child inherits a copy of its parent's connection configuration, including its maintenance database. Cleanup and failure-retention options are **not** inherited: constructor defaults apply unless overridden in `Derive`. All processes sharing templates must use the same maintenance database, because PostgreSQL advisory locks are database-scoped.

Newly initialized templates reject direct connections once migration is complete; use `Pool(t)` or clone a template for inspection. Older cached templates must also remain idle while cloning. Templates persist as reusable caches, and test cleanup drops only ephemeral databases. Do not administratively drop templates during active initialization; existing children are independent copies and remain usable after a parent is dropped.

## How It Works

Both approaches provide isolation, with different trade-offs:

- Transactions are faster but share the same database
- Separate databases provide complete isolation but are slightly slower

For a deeper dive, check out [segfaultmedaddy.com/p/pgxephemeraltest](https://segfaultmedaddy.com/p/pgxephemeraltest) for detailed information on the design and usage of this package.

> The package is deliberately built on top of the [pgx](https://github.com/jackc/pgx) driver and does not support the standard `database/sql` interface.
> If you need support for the standard `database/sql` interface, consider using [pgtestdb](https://github.com/peterldowns/pgtestdb).

## License

MIT Licensed
