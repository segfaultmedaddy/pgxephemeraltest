package pgxephemeraltest_test

import (
	"fmt"
	"math/rand/v2"
	"strconv"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"go.segfaultmedaddy.com/pgxephemeraltest"
)

func TestTxFactory(t *testing.T) {
	t.Parallel()

	// Arrange
	pf, err := pgxephemeraltest.NewPoolFactoryFromConnString(t.Context(), mkConnString(t), createKVMigrator())
	require.NoError(t, err)

	txf := pgxephemeraltest.NewTxFactory(pf.Pool(t))

	t.Run("it creates isolated transactions", func(t *testing.T) {
		t.Parallel()

		// Arrange
		tx1 := txf.Tx(t)
		tx2 := txf.Tx(t)

		k1, k2 := strconv.Itoa(rand.Int()), strconv.Itoa(rand.Int()) //#nosec:G404
		v1, v2 := strconv.Itoa(rand.Int()), strconv.Itoa(rand.Int()) //#nosec:G404

		// Act
		_, err = tx1.Exec(t.Context(), "INSERT INTO kv (key, value) VALUES ($1, $2)", k1, v1)
		require.NoError(t, err)

		_, err = tx2.Exec(t.Context(), "INSERT INTO kv (key, value) VALUES ($1, $2)", k2, v2)
		require.NoError(t, err)

		// Assert
		r1, err := tx1.Query(t.Context(), "SELECT * FROM kv")
		require.NoError(t, err)
		assertKVRows(t, r1, []kv{{k1, v1}})

		r2, err := tx2.Query(t.Context(), "SELECT * FROM kv")
		require.NoError(t, err)
		assertKVRows(t, r2, []kv{{k2, v2}})
	})
}

func TestTxFactory_Parallel(t *testing.T) {
	t.Parallel()

	// Arrange
	p, err := pgxpool.New(t.Context(), mkConnString(t))
	require.NoError(t, err)
	t.Cleanup(p.Close)

	f := pgxephemeraltest.NewTxFactory(p)

	for i := range 5 {
		t.Run(fmt.Sprintf("pool %d", i), func(t *testing.T) {
			t.Parallel()

			// Arrange
			tx := f.Tx(t)
			expectedValue := strconv.Itoa(i)

			_, err := tx.Exec(t.Context(), kvSchema)
			require.NoError(t, err)

			// Act
			_, err = tx.Exec(
				t.Context(),
				"INSERT INTO kv (key, value) VALUES ($1, $2)",
				"key",
				expectedValue,
			)
			require.NoError(t, err)

			// Assert
			rows, err := tx.Query(t.Context(), "SELECT * FROM kv")
			require.NoError(t, err)
			assertKVRows(t, rows, []kv{{"key", expectedValue}})
		})
	}
}
