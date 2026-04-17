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
	factory     *pgxephemeraltest.PoolFactory //nolint:gochecknoglobals
	factoryOnce sync.Once                     //nolint:gochecknoglobals
	errFactory  error                         //nolint:gochecknoglobals
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

	factoryOnce.Do(func() {
		factory, errFactory = pgxephemeraltest.NewPoolFactoryFromConnString(
			tb.Context(),
			os.Getenv("TEST_DATABASE_URL"),
			m,
		)
	})

	if errFactory != nil {
		tb.Fatal(errFactory)
	}

	return factory.Pool(tb)
}
