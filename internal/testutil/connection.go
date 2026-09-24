package testutil

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

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
