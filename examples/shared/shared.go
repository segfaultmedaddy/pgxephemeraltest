package shared

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"segfaultmedaddy.com/pgxephemeraltest"
)

var _ pgxephemeraltest.Migrator = (*migrator)(nil)

var m migrator //nolint:gochecknoglobals

var (
	mu sync.RWMutex                  //nolint:gochecknoglobals
	db *pgxephemeraltest.PoolFactory //nolint:gochecknoglobals
)

// Pool returns a pgxpool.Pool connected to ephemeral database.
//
// It lazily creates a pool factory connected to the database located at
// the given URL (TEST_DATABASE_URL env var).
func Pool(tb testing.TB) *pgxpool.Pool {
	tb.Helper()

	try := func() *pgxephemeraltest.PoolFactory {
		mu.RLock()
		defer mu.RUnlock()

		return db
	}

	get := func() *pgxephemeraltest.PoolFactory {
		if f := try(); f != nil {
			return f
		}

		mu.Lock()
		defer mu.Unlock()

		var err error

		db, err = pgxephemeraltest.NewPoolFactoryFromConnString(
			tb.Context(),
			os.Getenv("TEST_DATABASE_URL"),
			m,
		)
		if err != nil {
			tb.Fatal(err)
		}

		return db
	}

	return get().Pool(tb)
}

type migrator struct{}

func (migrator) Migrate(ctx context.Context, conn *pgx.Conn) error {
	_, err := conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS kv (
			key TEXT PRIMARY KEY NOT NULL,
			value INTEGER NOT NULL
		);
	`)

	return fmt.Errorf("failed to apply migration: %w", err)
}

func (migrator) Hash() string { return "shared" }
