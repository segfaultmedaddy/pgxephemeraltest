package migrator_test

import (
	"fmt"
	"io/fs"
	"testing"
	"testing/fstest"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.segfaultmedaddy.com/pgxephemeraltest/v2/internal/testutil"
	"go.segfaultmedaddy.com/pgxephemeraltest/v2/pkg/migrator"
)

type failingFS struct {
	fs.FS

	path string
}

func (f failingFS) Open(name string) (fs.File, error) {
	if name == f.path {
		return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrPermission}
	}

	file, err := f.FS.Open(name)
	if err != nil {
		return nil, fmt.Errorf("open test filesystem path %q: %w", name, err)
	}

	return file, nil
}

func Test_FSMigrator_Hash(t *testing.T) {
	t.Parallel()

	// Arrange
	base, err := migrator.FromFS(fstest.MapFS{
		"001.sql": {Data: []byte("ab")},
		"002.sql": {Data: []byte("c")},
	})
	require.NoError(t, err)

	baseHash := base.Hash()

	tests := []struct {
		fsys fstest.MapFS
		name string
		same bool
	}{
		{
			name: "should ignore map insertion order",
			fsys: fstest.MapFS{
				"002.sql": {Data: []byte("c")},
				"001.sql": {Data: []byte("ab")},
			},
			same: true,
		},
		{
			name: "should ignore non-SQL files",
			fsys: fstest.MapFS{
				"001.sql":   {Data: []byte("ab")},
				"002.sql":   {Data: []byte("c")},
				"README.md": {Data: []byte("migration documentation")},
			},
			same: true,
		},
		{
			name: "should distinguish identical contents split across different file boundaries",
			fsys: fstest.MapFS{
				"001.sql": {Data: []byte("a")},
				"002.sql": {Data: []byte("bc")},
			},
		},
		{
			name: "should change when a migration is renamed",
			fsys: fstest.MapFS{
				"001.sql": {Data: []byte("ab")},
				"003.sql": {Data: []byte("c")},
			},
		},
		{
			name: "should change when migrations are reordered",
			fsys: fstest.MapFS{
				"001.sql": {Data: []byte("c")},
				"002.sql": {Data: []byte("ab")},
			},
		},
		{
			name: "should change when an empty migration is added",
			fsys: fstest.MapFS{
				"001.sql": {Data: []byte("ab")},
				"002.sql": {Data: []byte("c")},
				"003.sql": {},
			},
		},
		{
			name: "should change when a migration is removed",
			fsys: fstest.MapFS{
				"001.sql": {Data: []byte("ab")},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			m, err := migrator.FromFS(tt.fsys)
			require.NoError(t, err)

			// Act
			hash := m.Hash()

			// Assert
			if tt.same {
				assert.Equal(t, baseHash, hash)
			} else {
				assert.NotEqual(t, baseHash, hash)
			}
		})
	}

	t.Run("should keep the original hash after filesystem changes", func(t *testing.T) {
		t.Parallel()

		// Arrange
		fsys := fstest.MapFS{
			"001.sql": {Data: []byte("SELECT 1;")},
		}
		m, err := migrator.FromFS(fsys)
		require.NoError(t, err)

		originalHash := m.Hash()
		fsys["001.sql"].Data = []byte("SELECT 2;")
		updated, err := migrator.FromFS(fsys)
		require.NoError(t, err)

		// Act
		hash := m.Hash()

		// Assert
		assert.Equal(t, originalHash, hash)
		assert.NotEqual(t, updated.Hash(), hash)
	})
}

func Test_FSMigrator_FromFS(t *testing.T) {
	t.Parallel()

	// Arrange
	tests := []struct {
		name string
		path string
		op   string
	}{
		{
			name: "should report errors opening the filesystem root",
			path: ".",
			op:   "walk migration path",
		},
		{
			name: "should report errors reading a nested directory",
			path: "nested",
			op:   "walk migration path",
		},
		{
			name: "should report errors reading a migration file",
			path: "nested/001.sql",
			op:   "read migration file",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			fsys := failingFS{
				FS: fstest.MapFS{
					"nested/001.sql": {Data: []byte("SELECT 1;")},
				},
				path: tt.path,
			}

			// Act
			m, err := migrator.FromFS(fsys)

			// Assert
			require.ErrorIs(t, err, fs.ErrPermission)
			require.ErrorContains(t, err, tt.op)
			assert.Nil(t, m)
			assert.ErrorContains(t, err, tt.path)
		})
	}

	t.Run("should accept an empty filesystem", func(t *testing.T) {
		t.Parallel()

		// Arrange
		fsys := fstest.MapFS{}

		// Act
		m, err := migrator.FromFS(fsys)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, m)
		assert.NotEmpty(t, m.Hash())
	})
}

func Test_FSMigrator_Migrate(t *testing.T) {
	t.Parallel()

	t.Run(
		"should apply nested migrations and their statements in lexicographic order",
		func(t *testing.T) {
			t.Parallel()

			// Arrange
			fsys := fstest.MapFS{
				"100-final.sql": {Data: []byte(`
DO $body$
BEGIN
    INSERT INTO migration_log (value) VALUES ('semi;colon');
END;
$body$;`)},
				"030/001.sql": {
					Data: []byte("INSERT INTO migration_log (value) VALUES ('nested');"),
				},
				"030.sql": {Data: []byte("INSERT INTO migration_log (value) VALUES ('flat');")},
				"020-seed.sql": {Data: []byte(`-- Seed data
INSERT INTO migration_log (value) VALUES ('first');
INSERT INTO migration_log (value) VALUES ('second');`)},
				"010-create.sql": {Data: []byte(`CREATE TEMP TABLE migration_log (
    position BIGINT GENERATED ALWAYS AS IDENTITY,
    value TEXT NOT NULL
);`)},
				"005-comments.sql": {Data: []byte("-- only a comment\n/* another comment */")},
				"README.md":        {Data: []byte("not SQL")},
				"link.sql":         {Data: []byte("not SQL"), Mode: fs.ModeSymlink},
			}
			m, err := migrator.FromFS(fsys)
			require.NoError(t, err)
			conn := testutil.Conn(t)

			// Act
			err = m.Migrate(t.Context(), conn)

			// Assert
			require.NoError(t, err)
			rows, err := conn.Query(
				t.Context(),
				"SELECT value FROM migration_log ORDER BY position",
			)
			require.NoError(t, err)
			values, err := pgx.CollectRows(rows, pgx.RowTo[string])
			require.NoError(t, err)
			assert.Equal(t, []string{"first", "second", "flat", "nested", "semi;colon"}, values)
		},
	)

	t.Run("should do nothing for an empty filesystem", func(t *testing.T) {
		t.Parallel()

		// Arrange
		m, err := migrator.FromFS(fstest.MapFS{})
		require.NoError(t, err)

		// Act
		err = m.Migrate(t.Context(), nil)

		// Assert
		require.NoError(t, err)
	})

	// Arrange
	tests := []struct {
		name     string
		sql      string
		error    string
		values   []string
		postgres bool
	}{
		{
			name:   "should stop when a migration cannot be split",
			sql:    "SELECT 'unclosed",
			error:  `split SQL migration "002-failure.sql"`,
			values: []string{"before"},
		},
		{
			name: "should stop when a statement cannot be executed",
			sql: `INSERT INTO migration_log (value) VALUES ('partial');
SELECT * FROM missing_migration_table;
INSERT INTO migration_log (value) VALUES ('unreachable');`,
			error:    `execute SQL migration "002-failure.sql" at 2:1`,
			values:   []string{"before", "partial"},
			postgres: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			m, err := migrator.FromFS(fstest.MapFS{
				"001-create.sql": {Data: []byte(`CREATE TEMP TABLE migration_log (
    position BIGINT GENERATED ALWAYS AS IDENTITY,
    value TEXT NOT NULL
);
INSERT INTO migration_log (value) VALUES ('before');`)},
				"002-failure.sql": {Data: []byte(tt.sql)},
				"003-later.sql": {
					Data: []byte("INSERT INTO migration_log (value) VALUES ('later');"),
				},
			})
			require.NoError(t, err)
			conn := testutil.Conn(t)

			// Act
			err = m.Migrate(t.Context(), conn)

			// Assert
			require.ErrorContains(t, err, tt.error)

			if tt.postgres {
				var pgErr *pgconn.PgError

				require.ErrorAs(t, err, &pgErr)
				assert.Equal(t, "42P01", pgErr.Code)
			}

			rows, err := conn.Query(
				t.Context(),
				"SELECT value FROM migration_log ORDER BY position",
			)
			require.NoError(t, err)
			values, err := pgx.CollectRows(rows, pgx.RowTo[string])
			require.NoError(t, err)
			assert.Equal(t, tt.values, values)
		})
	}
}

func Test_FSMigrator_FromFile(t *testing.T) {
	t.Parallel()

	t.Run("should load only the requested migration file", func(t *testing.T) {
		t.Parallel()

		// Arrange
		fsys := fstest.MapFS{
			"schema.sql": {Data: []byte(`CREATE TEMP TABLE single_migration (value INT);
INSERT INTO single_migration VALUES (42);`)},
			"other.sql": {Data: []byte("invalid SQL")},
		}
		conn := testutil.Conn(t)

		// Act
		m, err := migrator.FromFile(fsys, "schema.sql")
		require.NoError(t, err)
		err = m.Migrate(t.Context(), conn)

		// Assert
		require.NoError(t, err)

		var value int

		err = conn.QueryRow(t.Context(), "SELECT value FROM single_migration").Scan(&value)
		require.NoError(t, err)
		assert.Equal(t, 42, value)
	})
}
