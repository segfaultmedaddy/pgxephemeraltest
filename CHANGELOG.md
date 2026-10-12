# Changelog

## Unreleased

## v2.3.0

### Added

- **Lazy pool factories:** `WithLazyInitialization(true)` defers template initialization until the first `Pool` call. Derived factories inherit the setting and initialize their ancestors on demand. Initialization is concurrency-safe and can be retried after failures or cancellation; eager initialization remains the default.

## v2.2.0

### Added

- **Derived templates:** `PoolFactory.Derive` clones a parent template and applies a separate migrator. Parent-aware cache keys support nested derivation, and destination advisory locks coordinate concurrent initialization across processes without serializing siblings sharing a parent.

### Changed

- **Template names:** Root and derived templates now use full SHA-256 digests encoded with unpadded URL-safe Base64, with length-prefixed inputs and separate root/derived domains. The 59-byte names fit PostgreSQL's identifier limit without truncating the digest. Existing cached templates are not reused or automatically removed; migrations run again under the new names.
- **Template publication:** Newly initialized templates are disconnected before publication and reject direct connections afterward. Failed initialization attempts clean up unfinished databases while holding their advisory lock.
- **Derived factory options:** `Derive` inherits the parent's cleanup timeout and failure-retention settings. Explicit child options override inherited values without changing the parent or sibling factories.
- **Usage example:** The README demonstrates separate users schema and seed migrations, with a derived factory providing isolated copies of seeded data.
- **Derivation tests:** Derivation, factory options, and concurrency coverage are consolidated into parallel subtests. Tests run entirely in-process without launching subprocesses.

## v2.1.0

### Added

- **SQL statement splitter:** `pkg/sqlsplit` splits PostgreSQL SQL into statements, tracks their locations, and formats statement highlights.
- **Filesystem migrator:** Public `pkg/migrator` provides `FSMigrator`, with `FromFS` for recursively loading regular `.sql` files in lexicographic full-path order and `FromFile` for a single migration. Files are snapshotted when the migrator is constructed and applied sequentially.

### Changed

- **SQL migration parsing:** The migrator now uses `pkg/sqlsplit`, removing the `go.inout.gg/conduit` dependency.
- **Migration template hashes:** Hashes include ordered filenames and file contents, so changes to either produce a different database template.
- **Migration errors:** Failures identify the migration file and, for execution errors, the statement location. Migration stops at the first error.

## v2.0.0

### Breaking changes

- **Go module path:** Import `go.segfaultmedaddy.com/pgxephemeraltest/v2` and update dependencies to the v2 module path.
- **Database names:** Isolated databases now use `pgepht_<factory-prefix>_<URL-escaped-test-name>_<counter>` instead of the old word-based names and `pgxephemeraltest_db_` prefix. Each factory generates an eight-character random hex prefix; long test names are shortened to fit PostgreSQL's identifier limit. Update any tooling that relies on the old names or prefix.
- **One database and pool per call:** Every `Pool(t)` call creates a fresh database and connection pool, even when called repeatedly for the same test. Keep and reuse the returned pool if you need to share state within a test.

### Added

- **Failure cleanup option:** `WithKeepDatabaseOnFailure(false)` drops isolated databases after failed tests. The default remains `true`, which retains them for debugging.
