package pgxephemeraltest_test

import (
	"fmt"
	"math/rand/v2"
	"strconv"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"segfaultmedaddy.com/pgxephemeraltest"
)

func TestTxFactory(t *testing.T) {
	t.Parallel()

	// We use pool factory here just to create preinitialized schema.
	pf, err := pgxephemeraltest.NewPoolFactoryFromConnString(t.Context(), mkConnString(t), createKVMigrator())
	require.NoError(t, err)

	txf := pgxephemeraltest.NewTxFactory(pf.Pool(t))

	t.Run("it creates isolated transactions", func(t *testing.T) {
		t.Parallel()

		tx1 := txf.Tx(t)
		tx2 := txf.Tx(t)

		k1, k2 := strconv.Itoa(rand.Int()), strconv.Itoa(rand.Int()) //#nosec:G404
		v1, v2 := strconv.Itoa(rand.Int()), strconv.Itoa(rand.Int()) //#nosec:G404

		_, err = tx1.Exec(t.Context(), "INSERT INTO kv (key, value) VALUES ($1, $2)", k1, v1)
		require.NoError(t, err)

		_, err = tx2.Exec(t.Context(), "INSERT INTO kv (key, value) VALUES ($1, $2)", k2, v2)
		require.NoError(t, err)

		r1, err := tx1.Query(t.Context(), "SELECT * FROM kv")
		require.NoError(t, err)

		r2, err := tx2.Query(t.Context(), "SELECT * FROM kv")
		require.NoError(t, err)

		assertKVRows(t, r1, []kv{{k1, v1}})
		assertKVRows(t, r2, []kv{{k2, v2}})
	})
}

func TestTxFactory_Parallel(t *testing.T) {
	t.Parallel()

	p, err := pgxpool.New(t.Context(), mkConnString(t))
	require.NoError(t, err)
	t.Cleanup(p.Close)

	f := pgxephemeraltest.NewTxFactory(p)

	for i := range 5 {
		t.Run(fmt.Sprintf("pool %d", i), func(t *testing.T) {
			t.Parallel()

			tx := f.Tx(t)

			_, err := tx.Exec(t.Context(), kvSchema)
			require.NoError(t, err)

			_, err = tx.Exec(
				t.Context(),
				"INSERT INTO kv (key, value) VALUES ($1, $2)",
				"key",
				strconv.Itoa(i),
			)
			require.NoError(t, err)

			rows, err := tx.Query(t.Context(), "SELECT * FROM kv")
			require.NoError(t, err)

			assertKVRows(t, rows, []kv{{"key", strconv.Itoa(i)}})
		})
	}
}
