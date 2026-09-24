# Changelog

## v2.0.0

### Breaking changes

- **Database names:** Isolated databases now use `pgepht_<factory-prefix>_<URL-escaped-test-name>_<counter>` instead of the old word-based names and `pgxephemeraltest_db_` prefix. Each factory generates an eight-character random hex prefix; long test names are shortened to fit PostgreSQL's identifier limit. Update any tooling that relies on the old names or prefix.
- **One database and pool per call:** Every `Pool(t)` call creates a fresh database and connection pool, even when called repeatedly for the same test. Keep and reuse the returned pool if you need to share state within a test.

### Added

- **Failure cleanup option:** `WithKeepDatabaseOnFailure(false)` drops isolated databases after failed tests. The default remains `true`, which retains them for debugging.
