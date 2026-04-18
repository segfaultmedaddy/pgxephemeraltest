package dbmanager_test

import (
	"math/rand/v2"
	"strconv"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.segfaultmedaddy.com/pgxephemeraltest/internal/dbmanager"
	"go.segfaultmedaddy.com/pgxephemeraltest/internal/testutil"
)

func TestTemplateName(t *testing.T) {
	t.Parallel()

	// Arrange
	config, err := pgx.ParseConfig("postgres://user:pass@localhost:5432/postgres")
	require.NoError(t, err)

	m1 := testutil.NewMigrator("", "h1")
	m2 := testutil.NewMigrator("", "h2")

	// Act
	templateName := dbmanager.TemplateName(config, m1)

	// Assert
	assert.Equal(t, templateName, dbmanager.TemplateName(config, m1))
	assert.NotEqual(t, templateName, dbmanager.TemplateName(config, m2))
	assert.True(t, strings.HasPrefix(templateName, dbmanager.TemplatePrefix))

	config2 := config.Copy()
	config2.User = "another"
	config2.Password = "another"

	assert.NotEqual(t, templateName, dbmanager.TemplateName(config2, m1))
}

func TestDBManager(t *testing.T) {
	t.Parallel()

	// Arrange
	ctx := t.Context()
	config := testutil.PoolConfig(t)

	m, err := dbmanager.New(ctx, config)
	require.NoError(t, err)

	t.Run("it initializes template once and handles lifecycle", func(t *testing.T) {
		t.Parallel()

		// Arrange
		migrator := testutil.NewMigrator(
			testutil.KVSchema,
			"kv-"+strconv.FormatInt(rand.Int64(), 10),
		) // #nosec G404
		tpl := dbmanager.TemplateName(config.ConnConfig, migrator)
		dbName := "it_" + strconv.FormatInt(rand.Int64(), 10) // #nosec G404

		// Act
		err = m.Init(ctx, migrator, tpl)
		require.NoError(t, err)

		err = m.Init(ctx, migrator, tpl)
		require.NoError(t, err)

		createdDB, err := m.CreateDB(ctx, tpl, dbName)
		require.NoError(t, err)

		// Assert
		assert.Equal(t, int32(1), migrator.Calls(), "second Init call should reuse existing template")
		assert.Equal(t, dbmanager.DatabasePrefix+dbName, createdDB)

		conn := requireConnect(t, config, createdDB)

		_, err = conn.Exec(ctx, "INSERT INTO kv (key, value) VALUES ($1, $2)", "foo", "bar")
		require.NoError(t, err)

		var value string

		err = conn.QueryRow(ctx, "SELECT value FROM kv WHERE key = $1", "foo").Scan(&value)
		require.NoError(t, err)
		assert.Equal(t, "bar", value)

		conn.Close(ctx)

		dbs, err := m.ListDBs(ctx)
		require.NoError(t, err)
		assert.Contains(t, dbs, dbmanager.DBInfo{Name: tpl, IsTemplate: true})
		assert.Contains(t, dbs, dbmanager.DBInfo{Name: createdDB, IsTemplate: false})

		err = m.DropDB(ctx, createdDB, false)
		require.NoError(t, err)

		requireFailToConnect(t, config, createdDB, "connection should fail because the database was dropped")

		err = m.DropDB(ctx, tpl, true)
		require.NoError(t, err)

		requireFailToConnect(t, config, tpl, "connection should fail because the template database was dropped")
	})

	t.Run("it drops multiple databases including templates", func(t *testing.T) {
		t.Parallel()

		// Arrange
		migrator := testutil.NewMigrator(
			testutil.KVSchema,
			"kv-bulk-"+strconv.FormatInt(rand.Int64(), 10),
		) // #nosec G404
		tpl := dbmanager.TemplateName(config.ConnConfig, migrator)
		db1Suffix := "bulk1_" + strconv.FormatInt(rand.Int64(), 10) // #nosec G404
		db2Suffix := "bulk2_" + strconv.FormatInt(rand.Int64(), 10) // #nosec G404

		require.NoError(t, m.Init(ctx, migrator, tpl))

		db1, err := m.CreateDB(ctx, tpl, db1Suffix)
		require.NoError(t, err)

		db2, err := m.CreateDB(ctx, tpl, db2Suffix)
		require.NoError(t, err)

		// Act
		err = m.DropDBs(ctx, []string{db1, db2, tpl})
		require.NoError(t, err)

		// Assert
		for _, db := range []string{db1, db2, tpl} {
			requireFailToConnect(t, config, db)
		}
	})

	t.Run("it rejects unmanaged database names", func(t *testing.T) {
		t.Parallel()

		err = m.DropDBs(ctx, []string{"postgres"})
		require.Error(t, err)
		require.ErrorContains(t, err, "refusing to drop unmanaged database")
	})

	t.Run("it rejects unmanaged database name for single drop", func(t *testing.T) {
		t.Parallel()

		err = m.DropDB(ctx, "postgres", false)
		require.Error(t, err)
		require.ErrorContains(t, err, "refusing to drop unmanaged database")
	})
}

func requireConnect(tb testing.TB, config *pgxpool.Config, db string) *pgx.Conn {
	tb.Helper()

	connConfig := config.ConnConfig.Copy()
	connConfig.Database = db

	conn, err := pgx.ConnectConfig(tb.Context(), connConfig)
	require.NoError(tb, err)

	return conn
}

func requireFailToConnect(tb testing.TB, config *pgxpool.Config, db string, msg ...string) {
	tb.Helper()

	connConfig := config.ConnConfig.Copy()
	connConfig.Database = db

	conn, err := pgx.ConnectConfig(tb.Context(), connConfig)
	if conn != nil {
		conn.Close(tb.Context())
	}

	require.Error(tb, err, msg)
}
