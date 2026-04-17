package pgxephemeraltest_test

import (
	"fmt"
	"strconv"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"go.segfaultmedaddy.com/pgxephemeraltest"
	"go.segfaultmedaddy.com/pgxephemeraltest/internal/internaltesting"
	"go.segfaultmedaddy.com/pgxephemeraltest/internal/testutil"
)

func TestNewPoolFactory(t *testing.T) {
	t.Parallel()

	// Arrange
	config := testutil.PoolConfig(t)
	migrator := testutil.NewKVMigrator()

	// Act
	f, err := pgxephemeraltest.NewPoolFactory(t.Context(), config, migrator)

	// Assert
	require.NoError(t, err)
	assert.NotNil(t, f)

	err = f.Pool(t).Ping(t.Context())
	require.NoError(t, err)
}

func TestNewPoolFactoryFromConnString(t *testing.T) {
	t.Parallel()

	// Arrange
	connString := testutil.ConnString(t)
	migrator := testutil.NewKVMigrator()

	// Act
	f, err := pgxephemeraltest.NewPoolFactoryFromConnString(t.Context(), connString, migrator)

	// Assert
	require.NoError(t, err)
	assert.NotNil(t, f)

	err = f.Pool(t).Ping(t.Context())
	require.NoError(t, err)
}

func TestPoolFactory(t *testing.T) {
	t.Parallel()

	config := testutil.PoolConfig(t)

	f, err := pgxephemeraltest.NewPoolFactory(t.Context(), config, testutil.NewKVMigrator())
	require.NoError(t, err)

	t.Run("it cleans up database on success", func(t *testing.T) {
		t.Parallel()

		// Arrange
		var (
			database string
			cleanup  func()
			ctrl     = gomock.NewController(t)
			tt       = internaltesting.NewMockTB(ctrl)
		)

		tt.EXPECT().Context().AnyTimes().Return(t.Context())
		tt.EXPECT().Cleanup(gomock.Any()).AnyTimes().Do(func(f func()) {
			cleanup = f
		})
		tt.EXPECT().Helper().AnyTimes()
		tt.EXPECT().Logf(gomock.Any(), gomock.Any()).AnyTimes()
		tt.EXPECT().Failed().Times(1).Return(false)

		// Act
		pool := f.Pool(tt)
		database = pool.Config().ConnConfig.Database

		_, err = pool.Exec(t.Context(), "INSERT INTO kv (key, value) VALUES ($1, $2)", "foo", "bar")
		require.NoError(t, err)

		rows, err := pool.Query(t.Context(), "SELECT * FROM kv")
		require.NoError(t, err)
		testutil.AssertKVRows(t, rows, []testutil.KV{{Key: "foo", Value: "bar"}})

		require.NotNil(t, cleanup)
		cleanup()

		// Assert
		require.NotEmpty(t, database)

		cfg := config.Copy().ConnConfig.Copy()
		cfg.Database = database

		_, err = pgx.ConnectConfig(t.Context(), cfg)
		require.Error(t, err, "connection should fail because the database was dropped")
	})

	t.Run("it leaves database intact on failure", func(t *testing.T) {
		t.Parallel()

		// Arrange
		var (
			database string
			cleanup  func()
			ctrl     = gomock.NewController(t)
			tt       = internaltesting.NewMockTB(ctrl)
		)

		tt.EXPECT().Context().AnyTimes().Return(t.Context())
		tt.EXPECT().Cleanup(gomock.Any()).AnyTimes().Do(func(f func()) {
			cleanup = f
		})
		tt.EXPECT().Helper().AnyTimes()
		tt.EXPECT().Logf(gomock.Any(), gomock.Any()).AnyTimes()
		tt.EXPECT().Failed().Times(1).Return(true)

		// Act
		pool := f.Pool(tt)
		database = pool.Config().ConnConfig.Database

		_, err = pool.Exec(t.Context(), "INSERT INTO kv (key, value) VALUES ($1, $2)", "foo", "bar")
		require.NoError(t, err)

		rows, err := pool.Query(t.Context(), "SELECT * FROM kv")
		require.NoError(t, err)
		testutil.AssertKVRows(t, rows, []testutil.KV{{Key: "foo", Value: "bar"}})

		require.NotNil(t, cleanup)
		cleanup()

		// Assert
		require.NotEmpty(t, database)

		cfg := config.Copy().ConnConfig.Copy()
		cfg.Database = database

		conn, err := pgx.ConnectConfig(t.Context(), cfg)
		require.NoError(t, err, "database should still exist after failed test")
		t.Cleanup(func() { conn.Close(t.Context()) })

		rows, err = conn.Query(t.Context(), "SELECT * FROM kv")
		require.NoError(t, err)
		testutil.AssertKVRows(t, rows, []testutil.KV{{Key: "foo", Value: "bar"}})
	})

	t.Run("it creates an isolated database on each Pool call", func(t *testing.T) {
		t.Parallel()

		// Arrange
		p1 := f.Pool(t)
		p2 := f.Pool(t)

		// Act
		_, err := p1.Exec(t.Context(), "INSERT INTO kv (key, value) VALUES ($1, $2)", "key1", "value1")
		require.NoError(t, err)

		_, err = p2.Exec(t.Context(), "INSERT INTO kv (key, value) VALUES ($1, $2)", "key2", "value2")
		require.NoError(t, err)

		// Assert
		r1, err := p1.Query(t.Context(), "SELECT * FROM kv")
		require.NoError(t, err)
		testutil.AssertKVRows(t, r1, []testutil.KV{{Key: "key1", Value: "value1"}})

		r2, err := p2.Query(t.Context(), "SELECT * FROM kv")
		require.NoError(t, err)
		testutil.AssertKVRows(t, r2, []testutil.KV{{Key: "key2", Value: "value2"}})
	})

	t.Run("it supports creating multiple pools in parallel", func(t *testing.T) {
		t.Parallel()

		// Arrange
		var wg sync.WaitGroup

		// Act & Assert (per goroutine)
		for i := range 10 {
			wg.Go(func() {
				// Arrange
				p := f.Pool(t)
				expectedValue := strconv.Itoa(i)

				// Act
				_, err := p.Exec(
					t.Context(),
					"INSERT INTO kv (key, value) VALUES ($1, $2)",
					"key",
					expectedValue,
				)
				require.NoError(t, err)

				// Assert
				rows, err := p.Query(t.Context(), "SELECT * FROM kv")
				require.NoError(t, err)
				testutil.AssertKVRows(t, rows, []testutil.KV{{Key: "key", Value: expectedValue}})
			})
		}

		wg.Wait()
	})
}

func TestPoolFactory_Parallel(t *testing.T) {
	t.Parallel()

	// Arrange
	f, err := pgxephemeraltest.NewPoolFactoryFromConnString(
		t.Context(),
		testutil.ConnString(t),
		testutil.NewKVMigrator(),
	)
	require.NoError(t, err)

	for i := range 5 {
		t.Run(fmt.Sprintf("pool %d", i), func(t *testing.T) {
			t.Parallel()

			// Arrange
			p := f.Pool(t)
			expectedValue := strconv.Itoa(i)

			// Act
			_, err := p.Exec(
				t.Context(),
				"INSERT INTO kv (key, value) VALUES ($1, $2)",
				"key",
				expectedValue,
			)
			require.NoError(t, err)

			// Assert
			rows, err := p.Query(t.Context(), "SELECT * FROM kv")
			require.NoError(t, err)
			testutil.AssertKVRows(t, rows, []testutil.KV{{Key: "key", Value: expectedValue}})
		})
	}
}

func TestPoolFactory_NoopMigrator(t *testing.T) {
	t.Parallel()

	// Arrange
	config := testutil.PoolConfig(t)

	f, err := pgxephemeraltest.NewPoolFactory(t.Context(), config, testutil.NewNoopMigrator())
	require.NoError(t, err)

	// Act
	p1 := f.Pool(t)
	p2 := f.Pool(t)
	p3 := f.Pool(t)

	// Assert
	databases := map[string]struct{}{
		p1.Config().ConnConfig.Database: {},
		p2.Config().ConnConfig.Database: {},
		p3.Config().ConnConfig.Database: {},
	}

	assert.Len(t, databases, 3, "each Pool call should create a unique database")

	for _, p := range []*pgxpool.Pool{p1, p2, p3} {
		err := p.Ping(t.Context())
		assert.NoError(t, err, "pool should be connected to a valid database")
	}
}
