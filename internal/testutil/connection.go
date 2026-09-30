package testutil

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

// Conn connects to the test database and closes the connection when
// the test completes, using a bounded background context for cleanup.
func Conn(tb testing.TB) *pgx.Conn {
	tb.Helper()

	conn, err := pgx.Connect(tb.Context(), ConnString(tb))
	require.NoError(tb, err)
	tb.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		require.NoError(tb, conn.Close(ctx))
	})

	return conn
}

// RequireConnect connects to db with a copy of config and fails the test if it cannot.
// The caller must close the returned connection.
func RequireConnect(
	ctx context.Context,
	tb testing.TB,
	config *pgxpool.Config,
	db string,
	msg ...any,
) *pgx.Conn {
	tb.Helper()

	connConfig := config.ConnConfig.Copy()
	connConfig.Database = db

	conn, err := pgx.ConnectConfig(ctx, connConfig)
	require.NoError(tb, err, msg...)

	return conn
}

// RequireFailToConnect fails the test if a connection to db succeeds.
func RequireFailToConnect(
	ctx context.Context,
	tb testing.TB,
	config *pgxpool.Config,
	db string,
	msg ...any,
) {
	tb.Helper()

	connConfig := config.ConnConfig.Copy()
	connConfig.Database = db

	conn, err := pgx.ConnectConfig(ctx, connConfig)
	if conn != nil {
		conn.Close(ctx)
	}

	require.Error(tb, err, msg...)
}
