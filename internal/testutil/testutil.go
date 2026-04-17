package testutil

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const KVSchema = `CREATE TABLE IF NOT EXISTS kv (
  key TEXT PRIMARY KEY NOT NULL,
  value TEXT NOT NULL
);`

type Migrator struct {
	Schema string
	HashID string
	calls  atomic.Int32
}

func NewNoopMigrator() *Migrator {
	//nolint:exhaustruct // calls counter intentionally starts from zero value.
	return &Migrator{Schema: "", HashID: "noop"}
}

func NewKVMigrator() *Migrator {
	//nolint:exhaustruct // calls counter intentionally starts from zero value.
	return &Migrator{
		Schema: KVSchema,
		HashID: "kv" + strconv.FormatInt(rand.Int64(), 10), // #nosec G404
	}
}

func NewMigrator(schema, hash string) *Migrator {
	//nolint:exhaustruct // calls counter intentionally starts from zero value.
	return &Migrator{Schema: schema, HashID: hash}
}

func (m *Migrator) Hash() string { return m.HashID }

func (m *Migrator) Migrate(ctx context.Context, conn *pgx.Conn) error {
	m.calls.Add(1)

	if m.Schema == "" {
		return nil
	}

	_, err := conn.Exec(ctx, m.Schema)
	if err != nil {
		return fmt.Errorf("failed to apply schema: %w", err)
	}

	return nil
}

func (m *Migrator) Calls() int32 { return m.calls.Load() }

func ConnString(tb testing.TB) string {
	tb.Helper()

	connString := os.Getenv("TEST_DATABASE_URL")
	require.NotEmpty(tb, connString, "TEST_DATABASE_URL environment variable not set")

	return connString
}

func PoolConfig(tb testing.TB) *pgxpool.Config {
	tb.Helper()

	config, err := pgxpool.ParseConfig(ConnString(tb))
	require.NoError(tb, err)

	return config
}

type KV struct {
	Key   string
	Value string
}

func AssertKVRows(t *testing.T, rows pgx.Rows, expected []KV) {
	t.Helper()

	count := 0
	actual := make([]KV, 0, len(expected))

	defer rows.Close()

	for rows.Next() {
		var key, value string

		err := rows.Scan(&key, &value)
		require.NoError(t, err)

		actual = append(actual, KV{Key: key, Value: value})
		count++
	}

	assert.Equal(t, len(expected), count)
	assert.Equal(t, expected, actual)
}
