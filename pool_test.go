package pgxephemeraltest_test

import (
	"fmt"
	"strconv"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"go.segfaultmedaddy.com/pgxephemeraltest"
	"go.segfaultmedaddy.com/pgxephemeraltest/internal/internaltesting"
)

func TestNewPoolFactory(t *testing.T) {
	t.Parallel()

	// Arrange
	config := mkPoolConfig(t)
	migrator := createKVMigrator()

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
	connString := mkConnString(t)
	migrator := createKVMigrator()

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

	config := mkPoolConfig(t)

	f, err := pgxephemeraltest.NewPoolFactory(t.Context(), config, createKVMigrator())
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
		assertKVRows(t, rows, []kv{{"foo", "bar"}})

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
		assertKVRows(t, rows, []kv{{"foo", "bar"}})

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
		assertKVRows(t, rows, []kv{{"foo", "bar"}})
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
		assertKVRows(t, r1, []kv{{"key1", "value1"}})

		r2, err := p2.Query(t.Context(), "SELECT * FROM kv")
		require.NoError(t, err)
		assertKVRows(t, r2, []kv{{"key2", "value2"}})
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
				assertKVRows(t, rows, []kv{{"key", expectedValue}})
			})
		}

		wg.Wait()
	})
}

func TestPoolFactory_Parallel(t *testing.T) {
	t.Parallel()

	// Arrange
	f, err := pgxephemeraltest.NewPoolFactoryFromConnString(t.Context(), mkConnString(t), createKVMigrator())
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
			assertKVRows(t, rows, []kv{{"key", expectedValue}})
		})
	}
}

func TestPoolFactory_UniqueTemplate(t *testing.T) {
	t.Parallel()

	t.Run("it should create unique templates for migrators with different hashes", func(t *testing.T) {
		t.Parallel()

		// Arrange
		m1 := createKVMigrator()
		m2 := createKVMigrator()
		config := mkPoolConfig(t)

		// Act
		f1, err := pgxephemeraltest.NewPoolFactory(t.Context(), config, m1)
		require.NoError(t, err)

		f2, err := pgxephemeraltest.NewPoolFactory(t.Context(), config, m2)
		require.NoError(t, err)

		// Assert
		assert.NotEqual(t, f1.Template(), f2.Template())
	})

	t.Run("it should create unique templates for different users", func(t *testing.T) {
		t.Parallel()

		// Arrange
		m := createKVMigrator()
		c1 := mkPoolConfig(t)
		c2 := mkPoolConfig(t)

		c1.ConnConfig.User = "u1"
		c1.ConnConfig.Password = "u1"
		c2.ConnConfig.User = "u2"
		c2.ConnConfig.Password = "u2"

		// Act
		f1, err := pgxephemeraltest.NewPoolFactory(t.Context(), c1, m)
		require.NoError(t, err)

		f2, err := pgxephemeraltest.NewPoolFactory(t.Context(), c2, m)
		require.NoError(t, err)

		// Assert
		assert.NotEqual(t, f1.Template(), f2.Template())
	})

	t.Run("it should create the same templates for migrators with the same hashes", func(t *testing.T) {
		t.Parallel()

		// Arrange
		m := createKVMigrator()
		connString := mkConnString(t)

		// Act
		f1, err := pgxephemeraltest.NewPoolFactoryFromConnString(t.Context(), connString, m)
		require.NoError(t, err)

		f2, err := pgxephemeraltest.NewPoolFactoryFromConnString(t.Context(), connString, m)
		require.NoError(t, err)

		// Assert
		assert.Equal(t, f1.Template(), f2.Template())
	})
}
