# pgxephemeraltest

Isolated database testing for Go using ephemeral transactions and databases.

pgxephemeraltest provides isolated test environments for database-backed functionality in Go. It exposes wrappers around two common testing patterns:

- **Ephemeral transactions**: Tests run in transactions that are rolled back after completion
- **Ephemeral databases**: Tests run in fully isolated databases created from templates

When using ephemeral databases, the package leverages PostgreSQL [templates](https://www.postgresql.org/docs/current/manage-ag-templatedbs.html) for a quick initialization of a new database.

## Usage

```sh
$ go get go.segfaultmedaddy.com/pgxephemeraltest
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

For more usage examples, check out the `examples` directory in the root of this repository.

## How It Works

Both approaches provide isolation, with different trade-offs:

- Transactions are faster but share the same database
- Separate databases provide complete isolation but are slightly slower

For a deeper dive, check out [segfaultmedaddy.com/p/pgxephemeraltest](https://segfaultmedaddy.com/p/pgxephemeraltest) for detailed information on the design and usage of this package.

> The package is deliberately built on top of the [pgx](https://github.com/jackc/pgx) driver and does not support the standard `database/sql` interface.
> If you need support for the standard `database/sql` interface, consider using [pgtestdb](https://github.com/peterldowns/pgtestdb).

## License

MIT Licensed
