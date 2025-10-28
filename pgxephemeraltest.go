package pgxephemeraltest

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	_ Executor = (*pgx.Conn)(nil)
	_ Executor = (*pgxpool.Tx)(nil)
	_ Executor = (*pgxpool.Pool)(nil)
)

var DefaultCleanupTimeout = time.Second * 15 //nolint:gochecknoglobals

// Executor is an interface common to pgx.Conn, pgx.Tx and pgxpool.Pool.
type Executor interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
	CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error)
	SendBatch(context.Context, *pgx.Batch) pgx.BatchResults
	Begin(context.Context) (pgx.Tx, error)
}

// TB is the interface common to testing.T, testing.B, and testing.F.
//
// Copied from the testing package.
//
//nolint:interfacebloat // copied from testing package.
type TB interface {
	Cleanup(func())
	Error(args ...any)
	Errorf(format string, args ...any)
	Fail()
	FailNow()
	Failed() bool
	Fatal(args ...any)
	Fatalf(format string, args ...any)
	Helper()
	Log(args ...any)
	Logf(format string, args ...any)
	Name() string
	Setenv(key, value string)
	Chdir(dir string)
	Skip(args ...any)
	SkipNow()
	Skipf(format string, args ...any)
	Skipped() bool
	TempDir() string
	Context() context.Context
}

// FactoryOption is an option to configure PoolFactory and TxFactory.
type FactoryOption func(*factoryOptions)

// WithCleanupTimeout sets the timeout for cleaning up a database after
// a test is complete.
func WithCleanupTimeout(timeout time.Duration) func(*factoryOptions) {
	return func(config *factoryOptions) { config.cleanupTimeout = timeout }
}

// assertNoError is a helper function that asserts that an error is nil.
func assertNoError(t TB, err error, m ...string) {
	t.Helper()

	if err != nil {
		m := strings.Join(m, " ")
		if m != "" {
			err = fmt.Errorf("%s: %w", m, err)
		}

		t.Fatal(err)
	}
}
