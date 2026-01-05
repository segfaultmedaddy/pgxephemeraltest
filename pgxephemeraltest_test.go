package pgxephemeraltest_test

import (
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"go.segfaultmedaddy.com/pgxephemeraltest"
)

// BenchmarkTx_NewInstance assesses the performance of initialization of a new
// testing transaction.
func BenchmarkTx_NewInstance(b *testing.B) {
	config := mkPoolConfig(b)
	config.MaxConns = int32(b.N)

	pool, err := pgxpool.NewWithConfig(b.Context(), config)
	require.NoError(b, err)

	b.Cleanup(pool.Close)

	f := pgxephemeraltest.NewTxFactory(pool)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = f.Tx(b)
		}
	})
}

// BenchmarkPool_Template assesses the performance of the pool factory creation,
// that includes connection to the database for maintenance, initializing
// the template database.
func BenchmarkPool_Template(b *testing.B) {
	for b.Loop() {
		_, err := pgxephemeraltest.NewPoolFactory(b.Context(), mkPoolConfig(b), createKVMigrator())
		require.NoError(b, err)
	}
}

// BenchmarkPool_NewInstance assesses the performance of creating a new instance of
// pool with ignoring the template database creation time.
func BenchmarkPool_NewInstance(b *testing.B) {
	f, err := pgxephemeraltest.NewPoolFactory(b.Context(), mkPoolConfig(b), createKVMigrator())
	require.NoError(b, err)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = f.Pool(b)
		}
	})
}
