package list

import (
	"context"
	"math/rand/v2"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.segfaultmedaddy.com/pgxephemeraltest/v2/internal/dbmanager"
	"go.segfaultmedaddy.com/pgxephemeraltest/v2/internal/testutil"
)

func TestList(t *testing.T) {
	t.Parallel()

	// Arrange
	ctx := t.Context()
	connURL := testutil.ConnString(t)
	config := testutil.PoolConfig(t)

	m, err := dbmanager.New(ctx, config)
	require.NoError(t, err)

	t.Run("it lists databases without templates by default", func(t *testing.T) {
		t.Parallel()

		// Arrange
		id := strconv.FormatInt(rand.Int64(), 10) // #nosec G404
		migrator := testutil.NewMigrator("", "list-databases-"+id)
		tpl := dbmanager.TemplateName(config.ConnConfig, migrator)
		require.NoError(t, m.Init(ctx, migrator, tpl))

		db, err := m.CreateDB(ctx, tpl, "list_"+id)
		require.NoError(t, err)

		// Act
		result, err := list(ctx, args{ConnURL: connURL})
		require.NoError(t, err)

		// Assert
		dbs, ok := result.([]dbmanager.DBInfo)
		require.True(t, ok)
		assert.Contains(t, dbs, dbmanager.DBInfo{Name: db, IsTemplate: false})
		assert.NotContains(t, dbs, dbmanager.DBInfo{Name: tpl, IsTemplate: true})

		for _, info := range dbs {
			assert.False(t, info.IsTemplate)
		}

		require.NoError(t, m.DropDBs(ctx, []string{db, tpl}))
	})

	t.Run("it includes templates when requested", func(t *testing.T) {
		t.Parallel()

		// Arrange
		id := strconv.FormatInt(rand.Int64(), 10) // #nosec G404
		migrator := testutil.NewMigrator("", "list-templates-"+id)
		tpl := dbmanager.TemplateName(config.ConnConfig, migrator)
		require.NoError(t, m.Init(ctx, migrator, tpl))

		db, err := m.CreateDB(ctx, tpl, "list_"+id)
		require.NoError(t, err)

		// Act
		result, err := list(ctx, args{ConnURL: connURL, IncludeTemplate: true})
		require.NoError(t, err)

		// Assert
		dbs, ok := result.([]dbmanager.DBInfo)
		require.True(t, ok)
		assert.Contains(t, dbs, dbmanager.DBInfo{Name: db, IsTemplate: false})
		assert.Contains(t, dbs, dbmanager.DBInfo{Name: tpl, IsTemplate: true})

		require.NoError(t, m.DropDBs(ctx, []string{db, tpl}))
	})

	t.Run("it rejects invalid connection URLs", func(t *testing.T) {
		t.Parallel()

		// Act
		result, err := list(ctx, args{ConnURL: "://invalid"})

		// Assert
		assert.Nil(t, result)
		require.ErrorContains(t, err, "parse connection URL")
	})

	t.Run("it reports database connection errors", func(t *testing.T) {
		t.Parallel()

		// Arrange
		canceled, cancel := context.WithCancel(t.Context())
		cancel()

		// Act
		result, err := list(canceled, args{ConnURL: connURL})

		// Assert
		assert.Nil(t, result)
		require.ErrorContains(t, err, "list ephemeral databases")
	})
}
