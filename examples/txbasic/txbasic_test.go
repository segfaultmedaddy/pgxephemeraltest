package txbasic

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"go.segfaultmedaddy.com/pgxephemeraltest"
)

var factory *pgxephemeraltest.TxFactory //nolint:gochecknoglobals

func TestMain(m *testing.M) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)

	connString, ok := os.LookupEnv("TEST_DATABASE_URL")
	if !ok {
		panic("TEST_DATABASE_URL environment variable not set")
	}

	var err error

	f, closePool, err := pgxephemeraltest.NewTxFactoryFromConnString(ctx, connString)
	if err != nil {
		panic(err)
	}

	factory = f
	code := m.Run()

	closePool()
	cancel()
	os.Exit(code)
}

func TestTx(t *testing.T) {
	t.Parallel()

	for i := range 10 {
		t.Run(fmt.Sprintf("test_%d", i), func(t *testing.T) {
			t.Parallel()

			var (
				ctx = t.Context()
				tx  = factory.Tx(t)
			)

			_, err := tx.Exec(ctx, `
				CREATE TABLE IF NOT EXISTS kv (
					key TEXT PRIMARY KEY NOT NULL,
					value TEXT NOT NULL
				);
			`)
			require.NoError(t, err)

			_, err = tx.Exec(
				ctx,
				`insert into kv ("key", "value") values ($1, $2)`,
				"key",
				fmt.Sprintf("value_%d", i),
			)
			require.NoError(t, err)

			rows, err := tx.Query(ctx, "select * from kv")
			require.NoError(t, err)

			defer rows.Close()

			for rows.Next() {
				var key, value string

				err = rows.Scan(&key, &value)
				require.NoError(t, err)
				require.Equal(t, "key", key)
				require.Equal(t, fmt.Sprintf("value_%d", i), value)
			}

			require.NoError(t, rows.Err())
		})
	}
}
