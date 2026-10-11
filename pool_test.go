package pgxephemeraltest_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"golang.org/x/sync/errgroup"

	"go.segfaultmedaddy.com/pgxephemeraltest/v2"
	"go.segfaultmedaddy.com/pgxephemeraltest/v2/internal/dbmanager"
	"go.segfaultmedaddy.com/pgxephemeraltest/v2/internal/internaltesting"
	"go.segfaultmedaddy.com/pgxephemeraltest/v2/internal/testutil"
	"go.segfaultmedaddy.com/pgxephemeraltest/v2/pkg/migrator"
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
		tt.EXPECT().Name().Return(t.Name())
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

		testutil.RequireFailToConnect(
			t.Context(), t, config, database,
			"connection should fail because the database was dropped",
		)
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
		tt.EXPECT().Name().Return(t.Name())
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

		conn := testutil.RequireConnect(
			t.Context(), t, config, database,
			"database should still exist after failed test",
		)
		t.Cleanup(func() { conn.Close(t.Context()) })

		rows, err = conn.Query(t.Context(), "SELECT * FROM kv")
		require.NoError(t, err)
		testutil.AssertKVRows(t, rows, []testutil.KV{{Key: "foo", Value: "bar"}})
	})

	t.Run("it drops database on failure when configured", func(t *testing.T) {
		t.Parallel()

		factory, err := pgxephemeraltest.NewPoolFactory(
			t.Context(), config, testutil.NewNoopMigrator(),
			pgxephemeraltest.WithKeepDatabaseOnFailure(false),
		)
		require.NoError(t, err)

		var cleanup func()

		ctrl := gomock.NewController(t)
		tt := internaltesting.NewMockTB(ctrl)
		tt.EXPECT().Context().Return(t.Context())
		tt.EXPECT().Name().Return(t.Name())
		tt.EXPECT().Cleanup(gomock.Any()).Do(func(f func()) { cleanup = f })
		tt.EXPECT().Helper().AnyTimes()
		tt.EXPECT().Logf(gomock.Any(), gomock.Any()).AnyTimes()
		tt.EXPECT().Failed().Return(true)

		pool := factory.Pool(tt)
		database := pool.Config().ConnConfig.Database

		require.NotNil(t, cleanup)
		cleanup()

		testutil.RequireFailToConnect(
			t.Context(), t, config, database,
			"database should be dropped even when the test fails",
		)
	})

	t.Run("it creates an isolated database on each Pool call", func(t *testing.T) {
		t.Parallel()

		// Arrange
		p1 := f.Pool(t)
		p2 := f.Pool(t)

		// Act
		_, err := p1.Exec(
			t.Context(),
			"INSERT INTO kv (key, value) VALUES ($1, $2)",
			"key1",
			"value1",
		)
		require.NoError(t, err)

		_, err = p2.Exec(
			t.Context(),
			"INSERT INTO kv (key, value) VALUES ($1, $2)",
			"key2",
			"value2",
		)
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

func TestPoolFactory_DatabaseNames(t *testing.T) {
	t.Parallel()

	config := testutil.PoolConfig(t)
	f, err := pgxephemeraltest.NewPoolFactory(t.Context(), config, testutil.NewNoopMigrator())
	require.NoError(t, err)

	first := f.Pool(t).Config().ConnConfig.Database
	second := f.Pool(t).Config().ConnConfig.Database
	prefix := pgxephemeraltest.DatabasePrefix
	pattern := regexp.MustCompile("^" + regexp.QuoteMeta(prefix) + "[0-9a-f]{8}_")
	require.Regexp(t, pattern, first)
	namePrefix := first[:len(prefix)+8]
	assert.Equal(t, namePrefix+"_"+url.QueryEscape(t.Name())+"_1", first)
	assert.Equal(t, namePrefix+"_"+url.QueryEscape(t.Name())+"_2", second)

	other, err := pgxephemeraltest.NewPoolFactory(t.Context(), config, testutil.NewNoopMigrator())
	require.NoError(t, err)
	otherName := other.Pool(t).Config().ConnConfig.Database
	assert.Regexp(t, pattern, otherName)
	assert.Equal(t, "_"+url.QueryEscape(t.Name())+"_1", otherName[len(prefix)+8:])

	t.Run("long / name "+strings.Repeat("z", 100), func(t *testing.T) {
		t.Parallel()

		name := f.Pool(t).Config().ConnConfig.Database
		assert.LessOrEqual(t, len(name), 63)
		assert.True(t, strings.HasPrefix(name, namePrefix+"_"))
		assert.True(t, strings.HasSuffix(name, "_3"))
		assert.True(
			t,
			strings.HasPrefix(url.QueryEscape(t.Name()), name[len(namePrefix)+1:len(name)-2]),
		)
	})
}

func cleanupDatabase(t *testing.T, config *pgxpool.Config, db string) {
	t.Helper()

	m, err := dbmanager.New(t.Context(), config)
	require.NoError(t, err)

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		assert.NoError(t, m.DropDB(ctx, db))
	})
}

func TestPoolFactory_Derive(t *testing.T) {
	t.Parallel()

	config := testutil.PoolConfig(t)
	parentMigrator := testutil.NewMigrator(
		testutil.KVSchema+"INSERT INTO kv VALUES ('parent', 'base');",
		t.Name()+"-"+strconv.FormatInt(rand.Int64(), 10), // #nosec G404
	)
	parent, err := pgxephemeraltest.NewPoolFactory(t.Context(), config, parentMigrator)
	require.NoError(t, err)
	cleanupDatabase(t, config, parent.Template())

	t.Run(
		"it inherits parent schema and data and applies a different migrator",
		func(t *testing.T) {
			t.Parallel()

			// Arrange
			childMigrator, err := migrator.FromFS(fstest.MapFS{
				"feature.sql": {
					Data: []byte(
						"CREATE TABLE feature (id INTEGER); INSERT INTO kv VALUES ('child', 'feature');",
					),
				},
			})
			require.NoError(t, err)

			// Act
			child, err := parent.Derive(t.Context(), childMigrator)
			require.NoError(t, err)
			cleanupDatabase(t, config, child.Template())
			pool := child.Pool(t)

			// Assert
			assert.NotEqual(t, parent.Template(), child.Template())
			assert.Equal(
				t,
				int32(1),
				parentMigrator.Calls(),
				"parent migration must not be replayed",
			)

			rows, err := pool.Query(t.Context(), "SELECT key, value FROM kv ORDER BY key")
			require.NoError(t, err)
			testutil.AssertKVRows(t, rows, []testutil.KV{
				{Key: "child", Value: "feature"},
				{Key: "parent", Value: "base"},
			})

			var count int

			err = pool.QueryRow(t.Context(), "SELECT count(*) FROM feature").Scan(&count)
			require.NoError(t, err)
			assert.Zero(t, count)

			parentPool := parent.Pool(t)
			rows, err = parentPool.Query(t.Context(), "SELECT key, value FROM kv ORDER BY key")
			require.NoError(t, err)
			testutil.AssertKVRows(t, rows, []testutil.KV{{Key: "parent", Value: "base"}})

			var hasFeature bool

			err = parentPool.QueryRow(t.Context(), "SELECT to_regclass('feature') IS NOT NULL").
				Scan(&hasFeature)
			require.NoError(t, err)
			assert.False(t, hasFeature, "child schema must not leak into its parent")
		},
	)

	t.Run("it supports nested templates", func(t *testing.T) {
		t.Parallel()

		// Arrange
		childMigrator := testutil.NewMigrator(
			"CREATE TABLE feature (id INTEGER); INSERT INTO kv VALUES ('child', 'feature');",
			"nested-child",
		)
		child, err := parent.Derive(t.Context(), childMigrator)
		require.NoError(t, err)
		cleanupDatabase(t, config, child.Template())

		grandchildMigrator := testutil.NewMigrator(
			"INSERT INTO kv VALUES ('grandchild', 'nested');",
			"grandchild",
		)

		// Act
		grandchild, err := child.Derive(t.Context(), grandchildMigrator)
		require.NoError(t, err)
		cleanupDatabase(t, config, grandchild.Template())
		pool := grandchild.Pool(t)

		// Assert
		assert.NotEqual(t, child.Template(), grandchild.Template())
		assert.Equal(t, int32(1), childMigrator.Calls())
		assert.Equal(t, int32(1), grandchildMigrator.Calls())
		assert.Equal(t, int32(1), parentMigrator.Calls())

		rows, err := pool.Query(t.Context(), "SELECT key, value FROM kv ORDER BY key")
		require.NoError(t, err)
		testutil.AssertKVRows(t, rows, []testutil.KV{
			{Key: "child", Value: "feature"},
			{Key: "grandchild", Value: "nested"},
			{Key: "parent", Value: "base"},
		})

		var count int

		err = pool.QueryRow(t.Context(), "SELECT count(*) FROM feature").Scan(&count)
		require.NoError(t, err)
		assert.Zero(t, count, "grandchild must inherit the child's schema")
	})

	t.Run("it creates an isolated database on each child Pool call", func(t *testing.T) {
		t.Parallel()

		// Arrange
		child, err := parent.Derive(t.Context(), testutil.NewMigrator(
			"INSERT INTO kv VALUES ('child', 'feature');", "isolated-child",
		))
		require.NoError(t, err)
		cleanupDatabase(t, config, child.Template())
		p1 := child.Pool(t)
		p2 := child.Pool(t)

		// Act
		_, err = p1.Exec(t.Context(), "INSERT INTO kv VALUES ('ephemeral', 'isolated')")
		require.NoError(t, err)

		// Assert
		assert.NotEqual(t, p1.Config().ConnConfig.Database, p2.Config().ConnConfig.Database)

		r1, err := p1.Query(t.Context(), "SELECT key, value FROM kv ORDER BY key")
		require.NoError(t, err)
		testutil.AssertKVRows(t, r1, []testutil.KV{
			{Key: "child", Value: "feature"},
			{Key: "ephemeral", Value: "isolated"},
			{Key: "parent", Value: "base"},
		})

		r2, err := p2.Query(t.Context(), "SELECT key, value FROM kv ORDER BY key")
		require.NoError(t, err)
		testutil.AssertKVRows(t, r2, []testutil.KV{
			{Key: "child", Value: "feature"},
			{Key: "parent", Value: "base"},
		})
	})

	t.Run("it inherits a copied connection configuration", func(t *testing.T) {
		t.Parallel()

		// Arrange
		baseConfig := config.Copy()
		baseConfig.MaxConns = 2
		base, err := pgxephemeraltest.NewPoolFactory(
			t.Context(),
			baseConfig,
			testutil.NewKVMigrator(),
		)
		require.NoError(t, err)
		cleanupDatabase(t, config, base.Template())

		baseConfig.MaxConns = 3

		// Act
		child, err := base.Derive(t.Context(), testutil.NewNoopMigrator())
		require.NoError(t, err)
		cleanupDatabase(t, config, child.Template())
		pool := child.Pool(t)

		// Assert
		assert.Equal(t, int32(2), pool.Config().MaxConns)
		err = pool.Ping(t.Context())
		require.NoError(t, err)
	})

	t.Run("it reuses the same parent and local migration", func(t *testing.T) {
		t.Parallel()

		// Arrange
		childMigrator := testutil.NewMigrator("CREATE TABLE feature (id INTEGER);", "cached-child")
		child, err := parent.Derive(t.Context(), childMigrator)
		require.NoError(t, err)
		cleanupDatabase(t, config, child.Template())

		cachedMigrator := testutil.NewMigrator(
			"SELECT migration_must_not_run;",
			childMigrator.Hash(),
		)

		// Act
		cached, err := parent.Derive(t.Context(), cachedMigrator)

		// Assert
		require.NoError(t, err)
		assert.Equal(t, child.Template(), cached.Template())
		assert.Equal(t, int32(1), childMigrator.Calls())
		assert.Zero(t, cachedMigrator.Calls())
		err = cached.Pool(t).Ping(t.Context())
		require.NoError(t, err)
	})

	t.Run("it changes child templates when local migrations change", func(t *testing.T) {
		t.Parallel()

		// Arrange
		m1 := testutil.NewMigrator("INSERT INTO kv VALUES ('child', 'v1');", "local-v1")
		m2 := testutil.NewMigrator("INSERT INTO kv VALUES ('child', 'v2');", "local-v2")

		// Act
		c1, err := parent.Derive(t.Context(), m1)
		require.NoError(t, err)
		cleanupDatabase(t, config, c1.Template())
		c2, err := parent.Derive(t.Context(), m2)
		require.NoError(t, err)
		cleanupDatabase(t, config, c2.Template())

		// Assert
		assert.NotEqual(t, c1.Template(), c2.Template())
		assert.Equal(t, int32(1), m1.Calls())
		assert.Equal(t, int32(1), m2.Calls())

		var value string

		err = c1.Pool(t).
			QueryRow(t.Context(), "SELECT value FROM kv WHERE key = 'child'").
			Scan(&value)
		require.NoError(t, err)
		assert.Equal(t, "v1", value)
		err = c2.Pool(t).
			QueryRow(t.Context(), "SELECT value FROM kv WHERE key = 'child'").
			Scan(&value)
		require.NoError(t, err)
		assert.Equal(t, "v2", value)
	})

	t.Run("it carries parent changes into descendant identities", func(t *testing.T) {
		t.Parallel()

		// Arrange
		other, err := pgxephemeraltest.NewPoolFactory(t.Context(), config, testutil.NewMigrator(
			testutil.KVSchema+"INSERT INTO kv VALUES ('parent', 'updated');",
			t.Name()+"-"+strconv.FormatInt(rand.Int64(), 10), // #nosec G404
		))
		require.NoError(t, err)
		cleanupDatabase(t, config, other.Template())

		local := testutil.NewMigrator("", "same-local-migration")

		// Act
		c1, err := parent.Derive(t.Context(), local)
		require.NoError(t, err)
		cleanupDatabase(t, config, c1.Template())
		c2, err := other.Derive(t.Context(), local)
		require.NoError(t, err)
		cleanupDatabase(t, config, c2.Template())
		g1, err := c1.Derive(t.Context(), local)
		require.NoError(t, err)
		cleanupDatabase(t, config, g1.Template())
		g2, err := c2.Derive(t.Context(), local)
		require.NoError(t, err)
		cleanupDatabase(t, config, g2.Template())

		// Assert
		assert.NotEqual(t, c1.Template(), c2.Template())
		assert.NotEqual(t, g1.Template(), g2.Template())
		assert.NotEqual(t, c1.Template(), g1.Template())
		assert.NotEqual(t, c2.Template(), g2.Template())
		assert.Equal(t, int32(4), local.Calls())

		var value string

		err = g1.Pool(t).
			QueryRow(t.Context(), "SELECT value FROM kv WHERE key = 'parent'").
			Scan(&value)
		require.NoError(t, err)
		assert.Equal(t, "base", value)
		err = g2.Pool(t).
			QueryRow(t.Context(), "SELECT value FROM kv WHERE key = 'parent'").
			Scan(&value)
		require.NoError(t, err)
		assert.Equal(t, "updated", value)
	})

	t.Run("it does not reuse a root template with the same local migration", func(t *testing.T) {
		t.Parallel()

		// Arrange
		local := testutil.NewMigrator(
			"",
			t.Name()+"-"+strconv.FormatInt(rand.Int64(), 10),
		) // #nosec G404
		root, err := pgxephemeraltest.NewPoolFactory(t.Context(), config, local)
		require.NoError(t, err)
		cleanupDatabase(t, config, root.Template())

		// Act
		child, err := parent.Derive(t.Context(), local)
		require.NoError(t, err)
		cleanupDatabase(t, config, child.Template())

		// Assert
		assert.NotEqual(t, root.Template(), child.Template())
		assert.Equal(t, int32(2), local.Calls())

		rows, err := child.Pool(t).Query(t.Context(), "SELECT key, value FROM kv ORDER BY key")
		require.NoError(t, err)
		testutil.AssertKVRows(t, rows, []testutil.KV{{Key: "parent", Value: "base"}})
	})

	t.Run("it migrates different children sharing parents in parallel", func(t *testing.T) {
		t.Parallel()

		// Arrange
		parents := make([]*pgxephemeraltest.PoolFactory, 2)
		for i := range parents {
			parent, err := pgxephemeraltest.NewPoolFactory(
				t.Context(),
				config,
				testutil.NewKVMigrator(),
			)
			require.NoError(t, err)

			parents[i] = parent
			cleanupDatabase(t, config, parent.Template())
		}

		ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
		defer cancel()

		group, ctx := errgroup.WithContext(ctx)
		children := make([]*pgxephemeraltest.PoolFactory, 4)
		entered := make(chan string, len(children))
		release := make(chan struct{})

		migrations := make([]*testutil.Migrator, 2)
		for i := range migrations {
			migrations[i] = testutil.NewMigrator("", "parallel-child-"+strconv.Itoa(i))
			migrations[i].MigrateFunc = func(ctx context.Context, conn *pgx.Conn) error {
				if _, err := conn.Exec(
					ctx,
					"INSERT INTO kv VALUES ('child', $1)",
					strconv.Itoa(i),
				); err != nil {
					return fmt.Errorf("migrate child: %w", err)
				}

				entered <- conn.Config().Database

				select {
				case <-release:
					return nil
				case <-ctx.Done():
					return fmt.Errorf("wait for sibling migrations: %w", ctx.Err())
				}
			}
		}

		// Act
		for i, parent := range parents {
			for j, migration := range migrations {
				group.Go(func() error {
					child, err := parent.Derive(ctx, migration)
					if err != nil {
						return fmt.Errorf("derive child: %w", err)
					}

					children[i*len(migrations)+j] = child

					return nil
				})
			}
		}

		// Every migration must start before any can finish, proving overlap without timing assertions.
		started := make(map[string]struct{}, len(children))
		for range children {
			select {
			case template := <-entered:
				started[template] = struct{}{}
			case <-ctx.Done():
				require.NoError(t, group.Wait(), "children must migrate concurrently")
				t.Fatal("children did not reach the migration barrier")
			}
		}

		close(release)

		// Assert
		require.NoError(t, group.Wait())
		assert.Len(t, started, len(children))

		for _, migration := range migrations {
			assert.Equal(t, int32(2), migration.Calls())
		}

		for i, child := range children {
			cleanupDatabase(t, config, child.Template())

			var value string

			err := child.Pool(t).
				QueryRow(t.Context(), "SELECT value FROM kv WHERE key = 'child'").
				Scan(&value)
			require.NoError(t, err)
			assert.Equal(t, strconv.Itoa(i%len(migrations)), value)
		}
	})

	t.Run("it initializes shared parents and children once", func(t *testing.T) {
		t.Parallel()

		// Arrange
		parentMigrator := testutil.NewKVMigrator()
		childMigrator := testutil.NewMigrator("CREATE TABLE feature (id INTEGER);", "shared-child")
		parents := make([]*pgxephemeraltest.PoolFactory, 8)
		children := make([]*pgxephemeraltest.PoolFactory, len(parents))

		ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
		defer cancel()

		group, ctx := errgroup.WithContext(ctx)

		// Act
		for i := range children {
			group.Go(func() error {
				// Independent managers must coordinate through PostgreSQL, not a Go mutex.
				parent, err := pgxephemeraltest.NewPoolFactory(ctx, config, parentMigrator)
				if err != nil {
					return fmt.Errorf("initialize shared parent: %w", err)
				}

				child, err := parent.Derive(ctx, childMigrator)
				if err != nil {
					return fmt.Errorf("initialize shared child: %w", err)
				}

				parents[i] = parent
				children[i] = child

				return nil
			})
		}

		// Assert
		require.NoError(t, group.Wait())
		cleanupDatabase(t, config, parents[0].Template())
		cleanupDatabase(t, config, children[0].Template())
		assert.Equal(t, int32(1), parentMigrator.Calls())
		assert.Equal(t, int32(1), childMigrator.Calls())

		databases := make(map[string]struct{}, len(children))
		for i, child := range children {
			assert.Equal(t, parents[0].Template(), parents[i].Template())
			assert.Equal(t, children[0].Template(), child.Template())
			pool := child.Pool(t)
			databases[pool.Config().ConnConfig.Database] = struct{}{}

			var count int

			err := pool.QueryRow(t.Context(), "SELECT count(*) FROM feature").Scan(&count)
			require.NoError(t, err)
			assert.Zero(t, count)
		}

		assert.Len(
			t,
			databases,
			len(children),
			"each factory must have independent ephemeral database names",
		)
	})

	t.Run("it uses independent factory options", func(t *testing.T) {
		t.Parallel()

		// Arrange
		parent, err := pgxephemeraltest.NewPoolFactory(
			t.Context(), config, testutil.NewKVMigrator(),
			pgxephemeraltest.WithCleanupTimeout(time.Nanosecond),
			pgxephemeraltest.WithKeepDatabaseOnFailure(false),
		)
		require.NoError(t, err)
		cleanupDatabase(t, config, parent.Template())

		// Keep the shared template alive until all parallel option tests complete.
		child, err := parent.Derive(t.Context(), testutil.NewNoopMigrator())
		require.NoError(t, err)
		cleanupDatabase(t, config, child.Template())

		for _, tt := range []struct {
			wantCleanupError error
			name             string
			opts             []pgxephemeraltest.FactoryOption
			keep             bool
		}{
			{
				name: "it keeps a failed child's database by default",
				keep: true,
			},
			{
				name: "it applies child failure options without inheriting the parent's cleanup timeout",
				opts: []pgxephemeraltest.FactoryOption{pgxephemeraltest.WithKeepDatabaseOnFailure(false)},
			},
			{
				name: "it applies a child-specific cleanup timeout",
				opts: []pgxephemeraltest.FactoryOption{
					pgxephemeraltest.WithKeepDatabaseOnFailure(false),
					pgxephemeraltest.WithCleanupTimeout(time.Nanosecond),
				},
				keep:             true,
				wantCleanupError: context.DeadlineExceeded,
			},
		} {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()

				// Arrange
				factory, err := parent.Derive(t.Context(), testutil.NewNoopMigrator(), tt.opts...)
				require.NoError(t, err)

				var (
					cleanup    func()
					cleanupErr error
					ctrl       = gomock.NewController(t)
					tb         = internaltesting.NewMockTB(ctrl)
				)

				tb.EXPECT().Context().Return(t.Context())
				tb.EXPECT().Name().Return(t.Name())
				tb.EXPECT().Cleanup(gomock.Any()).Do(func(f func()) { cleanup = f })
				tb.EXPECT().Helper().AnyTimes()
				tb.EXPECT().Logf(gomock.Any(), gomock.Any()).AnyTimes()
				tb.EXPECT().Failed().Times(1).Return(true)

				if tt.wantCleanupError != nil {
					tb.EXPECT().
						Errorf(gomock.Any(), gomock.Any(), gomock.Any()).
						Do(func(_ string, args ...any) {
							cleanupErr, _ = args[1].(error)
						})
				}

				// Act
				pool := factory.Pool(tb)
				database := pool.Config().ConnConfig.Database

				require.NotNil(t, cleanup)
				cleanup()

				// Assert
				assert.Equal(
					t,
					child.Template(),
					factory.Template(),
					"options must not change the cached template",
				)

				if tt.wantCleanupError != nil {
					require.ErrorIs(t, cleanupErr, tt.wantCleanupError)
				} else {
					require.NoError(t, cleanupErr)
				}

				if tt.keep {
					cleanupDatabase(t, config, database)
					conn := testutil.RequireConnect(t.Context(), t, config, database,
						"failed test's database should remain available")
					require.NoError(t, conn.Close(t.Context()))
				} else {
					testutil.RequireFailToConnect(t.Context(), t, config, database,
						"failed test's database should be dropped with the child's options")
				}
			})
		}
	})
}

func TestPoolFactory_DeriveRecovery(t *testing.T) {
	t.Parallel()

	config := testutil.PoolConfig(t)
	parentMigrator := testutil.NewKVMigrator()
	parent, err := pgxephemeraltest.NewPoolFactory(t.Context(), config, parentMigrator)
	require.NoError(t, err)
	cleanupDatabase(t, config, parent.Template())

	t.Run("it retries a failed child without changing its parent or siblings", func(t *testing.T) {
		t.Parallel()

		// Arrange
		migrationErr := errors.New("child migration failed")

		childMigrator := testutil.NewMigrator("", "retry-child")
		childMigrator.MigrateFunc = func(ctx context.Context, conn *pgx.Conn) error {
			if _, err := conn.Exec(ctx,
				"CREATE TABLE feature (id INTEGER); INSERT INTO kv VALUES ('child', 'ready');",
			); err != nil {
				return fmt.Errorf("migrate child: %w", err)
			}

			if childMigrator.Calls() == 1 {
				return migrationErr
			}

			return nil
		}

		failed, err := parent.Derive(t.Context(), childMigrator)
		require.ErrorIs(t, err, migrationErr)
		require.Nil(t, failed)

		maintenance := testutil.Conn(t)
		childName := dbmanager.DerivedTemplateName(parent.Template(), childMigrator)

		var exists bool

		err = maintenance.QueryRow(t.Context(),
			"SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)", childName,
		).Scan(&exists)
		require.NoError(t, err)
		require.False(t, exists, "failed initialization should be cleaned up")

		// Act
		sibling, err := parent.Derive(t.Context(), testutil.NewNoopMigrator())
		require.NoError(t, err)
		cleanupDatabase(t, config, sibling.Template())
		child, err := parent.Derive(t.Context(), childMigrator)
		require.NoError(t, err)
		cleanupDatabase(t, config, child.Template())

		// Assert
		assert.Equal(t, childName, child.Template())
		assert.Equal(t, int32(2), childMigrator.Calls())
		assert.Equal(t, int32(1), parentMigrator.Calls())

		rows, err := child.Pool(t).Query(t.Context(), "SELECT key, value FROM kv ORDER BY key")
		require.NoError(t, err)
		testutil.AssertKVRows(t, rows, []testutil.KV{{Key: "child", Value: "ready"}})
		rows, err = parent.Pool(t).Query(t.Context(), "SELECT key, value FROM kv")
		require.NoError(t, err)
		testutil.AssertKVRows(t, rows, []testutil.KV{})
		rows, err = sibling.Pool(t).Query(t.Context(), "SELECT key, value FROM kv")
		require.NoError(t, err)
		testutil.AssertKVRows(t, rows, []testutil.KV{})
	})

	t.Run("it recovers an unfinished child template", func(t *testing.T) {
		t.Parallel()

		// Arrange
		childMigrator := testutil.NewMigrator(
			"CREATE TABLE feature (id INTEGER); INSERT INTO kv VALUES ('child', 'ready');",
			"unfinished-child",
		)
		childName := dbmanager.DerivedTemplateName(parent.Template(), childMigrator)
		maintenance := testutil.Conn(t)

		// Simulate a process crash leaving a partially migrated, unfinalized child.
		_, err := maintenance.Exec(
			t.Context(),
			"CREATE DATABASE "+pgx.Identifier{childName}.Sanitize()+
				" TEMPLATE "+pgx.Identifier{parent.Template()}.Sanitize(),
		)
		require.NoError(t, err)
		unfinished := testutil.RequireConnect(t.Context(), t, config, childName)
		_, err = unfinished.Exec(t.Context(),
			"CREATE TABLE feature (id INTEGER); INSERT INTO kv VALUES ('stale', 'partial');",
		)
		require.NoError(t, err)
		require.NoError(t, unfinished.Close(t.Context()))

		// Act
		child, err := parent.Derive(t.Context(), childMigrator)
		require.NoError(t, err)
		cleanupDatabase(t, config, child.Template())

		// Assert
		assert.Equal(t, childName, child.Template())
		assert.Equal(t, int32(1), childMigrator.Calls())
		assert.Equal(t, int32(1), parentMigrator.Calls())

		rows, err := child.Pool(t).Query(t.Context(), "SELECT key, value FROM kv ORDER BY key")
		require.NoError(t, err)
		testutil.AssertKVRows(t, rows, []testutil.KV{{Key: "child", Value: "ready"}})
	})

	t.Run("it releases the lock after a canceled migration", func(t *testing.T) {
		t.Parallel()

		// Arrange
		entered := make(chan struct{})
		result := make(chan error, 1)

		ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
		defer cancel()

		childMigrator := testutil.NewMigrator("", "cancel-child")
		childMigrator.MigrateFunc = func(ctx context.Context, conn *pgx.Conn) error {
			if childMigrator.Calls() == 1 {
				close(entered)
				<-ctx.Done()

				return fmt.Errorf("cancel migration: %w", ctx.Err())
			}

			if _, err := conn.Exec(ctx, "CREATE TABLE feature (id INTEGER)"); err != nil {
				return fmt.Errorf("retry migration: %w", err)
			}

			return nil
		}

		// Act
		go func() {
			_, err := parent.Derive(ctx, childMigrator)
			result <- err
		}()

		select {
		case <-entered:
		case <-ctx.Done():
			initErr := <-result
			require.NoError(t, initErr, "child should enter Migrate before cancellation")
			t.Fatal("child did not enter Migrate")
		}

		cancel()

		// Assert
		require.ErrorIs(t, <-result, context.Canceled)

		// Act
		retryCtx, retryCancel := context.WithTimeout(t.Context(), 15*time.Second)
		defer retryCancel()

		child, err := parent.Derive(retryCtx, childMigrator)
		require.NoError(t, err)
		cleanupDatabase(t, config, child.Template())

		// Assert
		assert.Equal(t, int32(2), childMigrator.Calls())
		err = child.Pool(t).Ping(t.Context())
		require.NoError(t, err)
	})
}
