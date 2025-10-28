package pgxephemeraltest

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TxFactory creates transactions for testing purposes.
//
// Transaction testing pattern is a powerful mechanism for testing database
// data. It spins up a transaction for each test case allowing to test database
// data in isolation concurrently. Once the test completes, the transaction is
// rolled back and the database state is reset to its initial state.
type TxFactory struct {
	executor Executor
}

// NewTxFactory creates a new TxFactory instance.
//
// The provided database connection pool must be open and ready to use.
func NewTxFactory(executor Executor, opts ...FactoryOption) TxFactory {
	//nolint:exhaustruct // defaults will initialize the missing fields.
	options := factoryOptions{}
	for _, opt := range opts {
		opt(&options)
	}

	options.defaults()

	return TxFactory{executor: executor}
}

// NewTxFactoryFromConnString creates a new connection pool from the provided connection string and
// returns a new TxFactory instance.
//
// A TxFactory instance is returned along with a cleanup function that
// should be called after the test suite completes.
func NewTxFactoryFromConnString(
	ctx context.Context,
	connString string,
	opts ...FactoryOption,
) (TxFactory, func(), error) {
	pool, err := pgxpool.New(ctx, connString)
	if err != nil {
		return TxFactory{}, nil, fmt.Errorf("pgxephemeraltest: failed to create connection pool: %w", err)
	}

	factory := NewTxFactory(pool, opts...)

	return factory, pool.Close, nil
}

// Tx spawns a database transaction for a given test. Once the test completes,
// the transaction is automatically rolled back on cleanup and the database
// state is reset to its initial state.
//
// If it fails to start a new transaction it will panic.
func (f TxFactory) Tx(tb TB) pgx.Tx {
	tb.Helper()

	tx, err := f.executor.Begin(tb.Context())
	assertNoError(tb, err, "pgxephemeraltest: failed to start transaction")

	tb.Logf("pgxephemeraltest: spun up new transaction for test")

	tb.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		if err = tx.Rollback(ctx); err != nil {
			if !errors.Is(err, pgx.ErrTxClosed) {
				assertNoError(tb, err, "pgxephemeraltest: failed to cleanup transaction")
			}
		}
	})

	return tx
}
