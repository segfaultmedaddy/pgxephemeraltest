package create

import (
	"io/fs"
	"math/rand/v2"
	"strconv"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.segfaultmedaddy.com/pgxephemeraltest/v2/internal/dbmanager"
	"go.segfaultmedaddy.com/pgxephemeraltest/v2/internal/testutil"
	"go.segfaultmedaddy.com/pgxephemeraltest/v2/pkg/migrator"
)

func TestCreate(t *testing.T) {
	t.Parallel()

	// Arrange
	ctx := t.Context()
	connURL := testutil.ConnString(t)
	config := testutil.PoolConfig(t)

	m, err := dbmanager.New(ctx, config)
	require.NoError(t, err)

	t.Run("it creates a template and database from a SQL file", func(t *testing.T) {
		t.Parallel()

		// Arrange
		id := strconv.FormatInt(rand.Int64(), 10) // #nosec G404
		fsys := fstest.MapFS{
			"schema.sql": {Data: []byte(testutil.KVSchema + "\nSELECT " + id + ";")},
		}
		fsMigrator, err := migrator.FromFile(fsys, "schema.sql")
		require.NoError(t, err)

		tpl := dbmanager.TemplateName(config.ConnConfig, fsMigrator)
		db := dbmanager.DatabasePrefix + "sql_" + id

		// Act
		result, err := create(ctx, fsys, args{
			ConnURL:      connURL,
			DatabaseName: "sql_" + id,
			FromSQL:      "schema.sql",
		})
		require.NoError(t, err)

		// Assert
		assert.Equal(t, []dbmanager.DBInfo{
			{Name: tpl, IsTemplate: true},
			{Name: db, IsTemplate: false},
		}, result)

		conn := testutil.RequireConnect(ctx, t, config, db)

		var count int

		err = conn.QueryRow(ctx, "SELECT count(*) FROM kv").Scan(&count)
		require.NoError(t, err)
		assert.Zero(t, count)
		conn.Close(ctx)

		require.NoError(t, m.DropDBs(ctx, []string{db, tpl}))
	})

	t.Run("it creates a database from an existing template", func(t *testing.T) {
		t.Parallel()

		// Arrange
		id := strconv.FormatInt(rand.Int64(), 10) // #nosec G404
		migration := testutil.NewMigrator(testutil.KVSchema, "create-existing-"+id)
		tpl := dbmanager.TemplateName(config.ConnConfig, migration)
		require.NoError(t, m.Init(ctx, migration, tpl))

		db := dbmanager.DatabasePrefix + "existing_" + id

		// Act
		result, err := create(ctx, fstest.MapFS{}, args{
			ConnURL:      connURL,
			DatabaseName: "existing_" + id,
			FromTemplate: tpl,
		})
		require.NoError(t, err)

		// Assert
		assert.Equal(t, []dbmanager.DBInfo{{Name: db, IsTemplate: false}}, result)

		conn := testutil.RequireConnect(ctx, t, config, db)

		var count int

		err = conn.QueryRow(ctx, "SELECT count(*) FROM kv").Scan(&count)
		require.NoError(t, err)
		assert.Zero(t, count)
		conn.Close(ctx)

		dbs, err := m.ListDBs(ctx)
		require.NoError(t, err)
		assert.Contains(t, dbs, dbmanager.DBInfo{Name: tpl, IsTemplate: true})

		require.NoError(t, m.DropDBs(ctx, []string{db, tpl}))
	})

	t.Run("it rejects missing SQL files", func(t *testing.T) {
		t.Parallel()

		// Act
		result, err := create(ctx, fstest.MapFS{}, args{ConnURL: connURL, FromSQL: "missing.sql"})

		// Assert
		assert.Nil(t, result)
		require.ErrorContains(t, err, "load SQL migration file")
		assert.ErrorIs(t, err, fs.ErrNotExist)
	})

	t.Run("it rejects missing templates", func(t *testing.T) {
		t.Parallel()

		// Arrange
		id := strconv.FormatInt(rand.Int64(), 10) // #nosec G404

		// Act
		result, err := create(ctx, fstest.MapFS{}, args{
			ConnURL:      connURL,
			DatabaseName: "missing_" + id,
			FromTemplate: dbmanager.TemplatePrefix + "missing_" + id,
		})

		// Assert
		assert.Nil(t, result)
		require.ErrorContains(t, err, "create ephemeral database from template")
	})

	t.Run("it rejects invalid connection URLs", func(t *testing.T) {
		t.Parallel()

		// Act
		result, err := create(ctx, fstest.MapFS{}, args{ConnURL: "://invalid"})

		// Assert
		assert.Nil(t, result)
		require.ErrorContains(t, err, "parse connection URL")
	})
}
