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

	"segfaultmedaddy.com/pgxephemeraltest"
)

func TestNewPoolFactory(t *testing.T) {
	t.Parallel()

	config, err := pgxpool.ParseConfig(mkConnString(t))
	require.NoError(t, err)

	f, err := pgxephemeraltest.NewPoolFactory(t.Context(), config, createKVMigrator())
	require.NoError(t, err)

	assert.NotNil(t, f)

	err = f.Pool(t).Ping(t.Context())
	require.NoError(t, err)
}

func TestNewPoolFactoryFromConnString(t *testing.T) {
	t.Parallel()

	f, err := pgxephemeraltest.NewPoolFactoryFromConnString(t.Context(), mkConnString(t), createKVMigrator())
	require.NoError(t, err)

	assert.NotNil(t, f)

	err = f.Pool(t).Ping(t.Context())
	require.NoError(t, err)
}

func TestPoolFactory(t *testing.T) {
	t.Parallel()

	config, err := pgxpool.ParseConfig(mkConnString(t))
	require.NoError(t, err)

	f, err := pgxephemeraltest.NewPoolFactory(t.Context(), config, createKVMigrator())
	require.NoError(t, err)

	t.Run("it cleans up database on success", func(t *testing.T) {
		t.Parallel()

		var (
			database string
			ctrl     = gomock.NewController(t)
			tt       = NewMockTB(ctrl)
		)

		var cleanup func()

		tt.EXPECT().Context().AnyTimes().Return(t.Context())
		tt.EXPECT().Cleanup(gomock.Any()).AnyTimes().Do(func(f func()) {
			cleanup = f
		})
		tt.EXPECT().Helper().AnyTimes()
		tt.EXPECT().Logf(gomock.Any(), gomock.Any()).AnyTimes()

		// Mock the Failed method to return false so the test passes.
		tt.EXPECT().Failed().Times(1).Return(false)

		pool := f.Pool(tt)

		// Take the database name so we can reconnect to it later to validate
		// the data.
		database = pool.Config().ConnConfig.Database
		require.NotEmpty(t, database)

		_, err = pool.Exec(t.Context(), "INSERT INTO kv (key, value) VALUES ($1, $2)", "foo", "bar")
		require.NoError(t, err)

		rows, err := pool.Query(t.Context(), "SELECT * FROM kv")
		require.NoError(t, err)

		assertKVRows(t, rows, []kv{{"foo", "bar"}})

		// Cleanup should drop the database on call.
		require.NotNil(t, cleanup)
		cleanup()

		cfg := config.Copy().ConnConfig.Copy()
		cfg.Database = database

		// Connection should fail because the database does not exist.
		_, err = pgx.ConnectConfig(t.Context(), cfg)
		require.Error(t, err)
	})

	t.Run("it leaves database intact on failure", func(t *testing.T) {
		t.Parallel()

		var (
			database string
			ctrl     = gomock.NewController(t)
			tt       = NewMockTB(ctrl)
		)

		var cleanup func()

		tt.EXPECT().Context().AnyTimes().Return(t.Context())
		tt.EXPECT().Cleanup(gomock.Any()).AnyTimes().Do(func(f func()) {
			cleanup = f
		})
		tt.EXPECT().Helper().AnyTimes()
		tt.EXPECT().Logf(gomock.Any(), gomock.Any()).AnyTimes()

		// Mock the Failed method to return true so the test pretends to fail.
		tt.EXPECT().Failed().Times(1).Return(true)

		pool := f.Pool(tt)

		// Take the database name so we can reconnect to it later to validate
		// the data.
		database = pool.Config().ConnConfig.Database
		require.NotEmpty(t, database)

		_, err = pool.Exec(t.Context(), "INSERT INTO kv (key, value) VALUES ($1, $2)", "foo", "bar")
		require.NoError(t, err)

		rows, err := pool.Query(t.Context(), "SELECT * FROM kv")
		require.NoError(t, err)

		assertKVRows(t, rows, []kv{{"foo", "bar"}})

		require.NotNil(t, cleanup)
		cleanup()

		cfg := config.Copy().ConnConfig.Copy()
		cfg.Database = database

		// Connect to the database associated with the failed test.
		conn, err := pgx.ConnectConfig(t.Context(), cfg)
		require.NoError(t, err)
		t.Cleanup(func() { conn.Close(t.Context()) })

		// Assert that the database has the expected rows.
		rows, err = conn.Query(t.Context(), "SELECT * FROM kv")
		require.NoError(t, err)

		assertKVRows(t, rows, []kv{{"foo", "bar"}})
	})

	t.Run("it creates an isolated database on each Pool call", func(t *testing.T) {
		t.Parallel()

		var (
			p1 = f.Pool(t)
			p2 = f.Pool(t)
		)

		_, err := p1.Exec(t.Context(), "INSERT INTO kv (key, value) VALUES ($1, $2)", "key1", "value1")
		require.NoError(t, err)

		_, err = p2.Exec(t.Context(), "INSERT INTO kv (key, value) VALUES ($1, $2)", "key2", "value2")
		require.NoError(t, err)

		r1, err := p1.Query(t.Context(), "SELECT * FROM kv")
		require.NoError(t, err)

		assertKVRows(t, r1, []kv{{"key1", "value1"}})

		r2, err := p2.Query(t.Context(), "SELECT * FROM kv")
		require.NoError(t, err)

		assertKVRows(t, r2, []kv{{"key2", "value2"}})
	})

	t.Run("it supports creating multiple pools in parallel", func(t *testing.T) {
		t.Parallel()

		var wg sync.WaitGroup
		for i := range 10 {
			wg.Go(func() {
				p := f.Pool(t)

				_, err := p.Exec(
					t.Context(),
					"INSERT INTO kv (key, value) VALUES ($1, $2)",
					"key",
					strconv.Itoa(i),
				)
				require.NoError(t, err)

				rows, err := p.Query(t.Context(), "SELECT * FROM kv")
				require.NoError(t, err)

				assertKVRows(t, rows, []kv{{"key", strconv.Itoa(i)}})
			})
		}

		wg.Wait()
	})
}

func TestPoolFactory_Parallel(t *testing.T) {
	t.Parallel()

	f, err := pgxephemeraltest.NewPoolFactoryFromConnString(t.Context(), mkConnString(t), createKVMigrator())
	require.NoError(t, err)

	for i := range 5 {
		t.Run(fmt.Sprintf("pool %d", i), func(t *testing.T) {
			t.Parallel()

			p := f.Pool(t)

			_, err := p.Exec(
				t.Context(),
				"INSERT INTO kv (key, value) VALUES ($1, $2)",
				"key",
				strconv.Itoa(i),
			)
			require.NoError(t, err)

			rows, err := p.Query(t.Context(), "SELECT * FROM kv")
			require.NoError(t, err)

			assertKVRows(t, rows, []kv{{"key", strconv.Itoa(i)}})
		})
	}
}

func TestPoolFactory_UniqueTemplate(t *testing.T) {
	t.Parallel()

	t.Run("it should create unique templates for migrators with different hashes", func(t *testing.T) {
		t.Parallel()

		var (
			m1 = createKVMigrator()
			m2 = createKVMigrator()
		)

		config, err := pgxpool.ParseConfig(mkConnString(t))
		require.NoError(t, err)

		f1, err := pgxephemeraltest.NewPoolFactory(t.Context(), config, m1)
		require.NoError(t, err)

		f2, err := pgxephemeraltest.NewPoolFactory(t.Context(), config, m2)
		require.NoError(t, err)

		assert.NotEqual(t, f1.Template(), f2.Template())
	})

	t.Run("it should create the same templates for migrators with the same hashes", func(t *testing.T) {
		t.Parallel()

		m := createKVMigrator()

		f1, err := pgxephemeraltest.NewPoolFactoryFromConnString(t.Context(), mkConnString(t), m)
		require.NoError(t, err)

		f2, err := pgxephemeraltest.NewPoolFactoryFromConnString(t.Context(), mkConnString(t), m)
		require.NoError(t, err)

		assert.Equal(t, f1.Template(), f2.Template())
	})
}
