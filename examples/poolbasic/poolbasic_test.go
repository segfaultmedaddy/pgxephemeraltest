package poolbasic

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"go.segfaultmedaddy.com/pgxephemeraltest"
)

var factory *pgxephemeraltest.PoolFactory //nolint:gochecknoglobals

// migrator applies test migrations and present here for example purposes.
type migrator struct{}

func (migrator) Migrate(ctx context.Context, conn *pgx.Conn) error {
	_, err := conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS kv (
			key TEXT PRIMARY KEY NOT NULL,
			value TEXT NOT NULL
		);
	`)
	if err != nil {
		return fmt.Errorf("failed to apply migration: %w", err)
	}

	return nil
}

func (migrator) Hash() string { return "basic" }

func TestMain(m *testing.M) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)

	connString, ok := os.LookupEnv("TEST_DATABASE_URL")
	if !ok {
		panic("TEST_DATABASE_URL environment variable not set")
	}

	var err error

	factory, err = pgxephemeraltest.NewPoolFactoryFromConnString(ctx, connString, &migrator{})
	if err != nil {
		panic(err)
	}

	code := m.Run()

	cancel()
	os.Exit(code)
}

func TestPool(t *testing.T) {
	t.Parallel()

	for i := range 10 {
		t.Run(fmt.Sprintf("test_%d", i), func(t *testing.T) {
			t.Parallel()

			var (
				ctx = t.Context()
				p   = factory.Pool(t)
			)

			_, err := p.Exec(
				ctx,
				`insert into kv ("key", "value") values ($1, $2)`,
				"key",
				fmt.Sprintf("value_%d", i),
			)
			require.NoError(t, err)

			rows, err := p.Query(ctx, "select * from kv")
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
