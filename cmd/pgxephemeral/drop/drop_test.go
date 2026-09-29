package drop

import (
	"math/rand/v2"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.segfaultmedaddy.com/pgxephemeraltest/v2/internal/dbmanager"
	"go.segfaultmedaddy.com/pgxephemeraltest/v2/internal/testutil"
)

func TestDrop(t *testing.T) {
	t.Parallel()

	// Arrange
	ctx := t.Context()
	connURL := testutil.ConnString(t)
	config := testutil.PoolConfig(t)

	m, err := dbmanager.New(ctx, config)
	require.NoError(t, err)

	t.Run("it drops only selected databases", func(t *testing.T) {
		t.Parallel()

		// Arrange
		id := strconv.FormatInt(rand.Int64(), 10) // #nosec G404
		migrator := testutil.NewMigrator("", "drop-selected-"+id)
		tpl := dbmanager.TemplateName(config.ConnConfig, migrator)

		require.NoError(t, m.Init(ctx, migrator, tpl))

		db1, err := m.CreateDB(ctx, tpl, "selected_"+id)
		require.NoError(t, err)

		db2, err := m.CreateDB(ctx, tpl, "retained_"+id)
		require.NoError(t, err)

		// Act
		result, err := drop(ctx, args{ConnURL: connURL, DatabaseNames: []string{db1}})
		require.NoError(t, err)

		// Assert
		assert.Equal(t, []dbmanager.DBInfo{{Name: db1, IsTemplate: false}}, result)
		testutil.RequireFailToConnect(ctx, t, config, db1)

		conn := testutil.RequireConnect(ctx, t, config, db2)
		conn.Close(ctx)

		dbs, err := m.ListDBs(ctx)
		require.NoError(t, err)
		assert.Contains(t, dbs, dbmanager.DBInfo{Name: tpl, IsTemplate: true})
		assert.Contains(t, dbs, dbmanager.DBInfo{Name: db2, IsTemplate: false})

		require.NoError(t, m.DropDBs(ctx, []string{db2, tpl}))
	})

	t.Run("it excludes templates by default", func(t *testing.T) {
		t.Parallel()

		// Arrange
		id := strconv.FormatInt(rand.Int64(), 10) // #nosec G404
		migrator := testutil.NewMigrator("", "drop-template-"+id)
		tpl := dbmanager.TemplateName(config.ConnConfig, migrator)

		require.NoError(t, m.Init(ctx, migrator, tpl))

		db, err := m.CreateDB(ctx, tpl, "template_"+id)
		require.NoError(t, err)

		// Act
		result, err := drop(ctx, args{ConnURL: connURL, DatabaseNames: []string{db, tpl}})
		require.NoError(t, err)

		// Assert
		assert.Equal(t, []dbmanager.DBInfo{{Name: db, IsTemplate: false}}, result)
		testutil.RequireFailToConnect(ctx, t, config, db)

		dbs, err := m.ListDBs(ctx)
		require.NoError(t, err)
		assert.Contains(t, dbs, dbmanager.DBInfo{Name: tpl, IsTemplate: true})

		require.NoError(t, m.DropDB(ctx, tpl))
	})

	t.Run("it drops templates when requested", func(t *testing.T) {
		t.Parallel()

		// Arrange
		id := strconv.FormatInt(rand.Int64(), 10) // #nosec G404
		migrator := testutil.NewMigrator("", "drop-included-template-"+id)
		tpl := dbmanager.TemplateName(config.ConnConfig, migrator)

		require.NoError(t, m.Init(ctx, migrator, tpl))

		// Act
		result, err := drop(
			ctx,
			args{ConnURL: connURL, DatabaseNames: []string{tpl}, IncludeTemplate: true},
		)
		require.NoError(t, err)

		// Assert
		assert.Equal(t, []dbmanager.DBInfo{{Name: tpl, IsTemplate: true}}, result)
		testutil.RequireFailToConnect(ctx, t, config, tpl)
	})

	t.Run("it rejects missing database names", func(t *testing.T) {
		t.Parallel()

		// Arrange
		name := dbmanager.DatabasePrefix + "missing_" + strconv.FormatInt(
			rand.Int64(),
			10,
		) // #nosec G404

		// Act
		result, err := drop(ctx, args{ConnURL: connURL, DatabaseNames: []string{name}})

		// Assert
		assert.Nil(t, result)
		require.ErrorContains(t, err, "no matching databases to drop")
	})

	t.Run("it rejects invalid connection URLs", func(t *testing.T) {
		t.Parallel()

		// Act
		result, err := drop(ctx, args{ConnURL: "://invalid"})

		// Assert
		assert.Nil(t, result)
		require.ErrorContains(t, err, "parse connection URL")
	})
}
