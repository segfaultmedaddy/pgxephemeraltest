package pgxephemeraltest_test

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"strconv"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.segfaultmedaddy.com/pgxephemeraltest"
)

var _ pgxephemeraltest.Migrator = (*migrator)(nil)

const kvSchema = `CREATE TABLE IF NOT EXISTS kv (
  key TEXT PRIMARY KEY NOT NULL,
  value TEXT NOT NULL
);`

// kvMigrator is a default migrator for tests.
func createKVMigrator() *migrator {
	return &migrator{
		schema: kvSchema,

		// provide a random hash so we create independent migrations in each parallel
		// test.
		//#nosec:G404
		hash: "kv" + strconv.FormatInt(rand.Int64(), 10),
	}
}

// mkConnString returns the connection string for testing.
//
// It expects TEST_DATABASE_URL environment variable to be set.
func mkConnString(tb testing.TB) string {
	tb.Helper()

	connString := os.Getenv("TEST_DATABASE_URL")
	require.NotEmpty(tb, connString, "TEST_DATABASE_URL environment variable not set")

	return connString
}

// mkPoolConfig returns a pool config for testing.
func mkPoolConfig(tb testing.TB) *pgxpool.Config {
	tb.Helper()

	config, err := pgxpool.ParseConfig(mkConnString(tb))
	require.NoError(tb, err)

	return config
}

type migrator struct {
	schema string
	hash   string
}

func (m *migrator) Hash() string { return m.hash }
func (m *migrator) Migrate(ctx context.Context, conn *pgx.Conn) error {
	_, err := conn.Exec(ctx, m.schema)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	return nil
}

type kv struct{ k, v string }

// assertKVRows asserts that the rows match the expected values.
func assertKVRows(t *testing.T, rows pgx.Rows, expected []kv) {
	t.Helper()

	count := 0
	actual := make([]kv, 0, len(expected))

	defer rows.Close()

	for rows.Next() {
		var k, v string

		err := rows.Scan(&k, &v)
		require.NoError(t, err)

		actual = append(actual, kv{k, v})
		count++
	}

	assert.Equal(t, len(expected), count)
	assert.Equal(t, expected, actual)
}
