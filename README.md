# pgxephemeraltest

pgxephemeraltest package allows to run database-related tests in Go in an isolated environment.

pgxephemeraltest exposes wrappers around two common testing patterns of database-backed functionality:

- Running a test in an ephemeral transaction
- Running a test in an ephemeral database

The package utilizes Postgres [template databases](https://www.postgresql.org/docs/current/manage-ag-templatedbs.html) for quick setup of fully isolated databases.

Check out [segfaultmedaddy.com/p/pgxephemeraltest](https://segfaultmedaddy.com/p/pgxephemeraltest) for detailed information on the design and usage of this package.

> The package is deliberately built on top of the [pgx](https://github.com/jackc/pgx) driver and does not support the standard `database/sql` interface.
> If you need support for the standard `database/sql` interface, consider using [pgtestdb](https://github.com/peterldowns/pgtestdb).

## License

MIT Licensed
