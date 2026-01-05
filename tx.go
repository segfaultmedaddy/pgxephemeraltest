package pgxephemeraltest

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"go.segfaultmedaddy.com/pgxephemeraltest/internal/internaltesting"
)

// TxFactory creates transactions for testing purposes.
//
// Transaction testing pattern is a powerful mechanism for testing database
// data. It spins up a transaction for each test case allowing to test database
// data in isolation concurrently. Once the test completes, the transaction is
// rolled back and the database state is reset to its initial state.
type TxFactory struct {
	executor Executor
	options  factoryOptions
}

// NewTxFactory creates a new TxFactory instance.
//
// The provided database connection pool must be open and ready to use.
func NewTxFactory(executor Executor, opts ...FactoryOption) *TxFactory {
	//nolint:exhaustruct // defaults will initialize the missing fields.
	options := factoryOptions{}
	for _, opt := range opts {
		opt(&options)
	}

	options.defaults()

	return &TxFactory{executor: executor, options: options}
}

// NewTxFactoryFromConnString creates a new connection pool from the provided connection string and
// returns a new TxFactory instance.
//
// A TxFactory instance is returned along with a cleanup function that
// should be called after testing is complete.
func NewTxFactoryFromConnString(
	ctx context.Context,
	connString string,
	opts ...FactoryOption,
) (*TxFactory, func(), error) {
	pool, err := pgxpool.New(ctx, connString)
	if err != nil {
		return nil, nil, fmt.Errorf("pgxephemeraltest: failed to create connection pool: %w", err)
	}

	f := NewTxFactory(pool, opts...)

	return f, pool.Close, nil
}

// Tx spawns a database transaction for a given test. Once the test completes,
// the transaction is automatically rolled back on cleanup and the database
// state is reset to its initial state.
//
// If it fails to start a new transaction a panic is raised.
func (f TxFactory) Tx(tb internaltesting.TB) pgx.Tx {
	tb.Helper()

	//nolint:exhaustruct
	tx, err := f.executor.BeginTx(tb.Context(), pgx.TxOptions{
		// ReadCommitted is the default isolation level in Postgres, however,
		// it might be overridden by the database configuration. We need to ensure
		// that the transaction isolation level doesn't allow dirty writes.
		IsoLevel: pgx.ReadCommitted,
	})
	assertNoError(tb, err, "pgxephemeraltest: failed to start transaction")

	tb.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), f.options.cleanupTimeout)
		defer cancel()

		// It is important to pass a fresh context here as the tb.Context()
		// is canceled when the test is finished.
		if err := tx.Rollback(ctx); err != nil {
			if !errors.Is(err, pgx.ErrTxClosed) {
				assertNoError(tb, err, "pgxephemeraltest: failed to cleanup transaction")
			} else {
				tb.Logf("pgxephemeraltest: transaction was already closed")
			}
		}
	})

	return tx
}
