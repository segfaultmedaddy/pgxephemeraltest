package sharedpool

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"go.segfaultmedaddy.com/pgxephemeraltest"
)

var _ pgxephemeraltest.Migrator = (*migrator)(nil)

var m migrator //nolint:gochecknoglobals

var (
	mu sync.RWMutex                  //nolint:gochecknoglobals
	db *pgxephemeraltest.PoolFactory //nolint:gochecknoglobals
)

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

func (migrator) Hash() string { return "shared" }

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
		// First check if the pool manager is already initialized...
		if db := try(); db != nil {
			return db
		}

		mu.Lock()
		defer mu.Unlock()

		var err error

		// ...otherwise create a new one connected to TEST_DATABASE_URL instance...
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

	// Finally return a new pool created from the pool factory.
	return get().Pool(tb)
}
