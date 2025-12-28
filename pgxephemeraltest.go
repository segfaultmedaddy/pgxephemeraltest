package pgxephemeraltest

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"go.segfaultmedaddy.com/pgxephemeraltest/internal/internaltesting"
)

var (
	_ Executor = (*pgx.Conn)(nil)
	_ Executor = (*pgxpool.Pool)(nil)
)

var DefaultCleanupTimeout = time.Second * 15 //nolint:gochecknoglobals

// Executor is an interface common to pgx.Conn and pgxpool.Pool.
type Executor interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
	CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error)
	SendBatch(context.Context, *pgx.Batch) pgx.BatchResults
	Begin(context.Context) (pgx.Tx, error)
	BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error)
}

// FactoryOption is an option to configure PoolFactory and TxFactory.
type FactoryOption func(*factoryOptions)

// WithCleanupTimeout sets the timeout for cleaning up a database after
// a test is complete.
func WithCleanupTimeout(timeout time.Duration) func(*factoryOptions) {
	return func(config *factoryOptions) { config.cleanupTimeout = timeout }
}

// assertNoError is a helper function that asserts that an error is nil.
func assertNoError(t internaltesting.TB, err error, m ...string) {
	t.Helper()

	if err != nil {
		m := strings.Join(m, " ")
		if m != "" {
			err = fmt.Errorf("%s: %w", m, err)
		}

		t.Fatal(err)
	}
}
