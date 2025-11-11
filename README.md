# pgxephemeraltest

pgxephemeraltest package provides a set of utility constructs for database-related tests in Go.

It exposes wrappers around two common testing patterns of database-backed functionality:

- Running a test in an ephemeral transaction
- Running a test in an ephemeral database

Check out [segfaultmedaddy.com/p/pgxephemeraltest](https://segfaultmedaddy.com/p/pgxephemeraltest) for detailed information on the design and usage of this package.

## License

## Acknowledgements

This package is inspired by [pgtestdb](https://github.com/peterldowns/pgtestdb) which supports the standard `database/sql` interface and provides adapters for many popular database migration tools.
