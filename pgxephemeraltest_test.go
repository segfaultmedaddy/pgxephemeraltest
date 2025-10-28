package pgxephemeraltest_test

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// mkConnString returns the connection string for testing.
// It expects TEST_DATABASE_URL environment variable to be set.
//
//nolint:unused // mkConnString is going to be used by the test suites
func mkConnString(t *testing.T) string {
	t.Helper()

	connString := os.Getenv("TEST_DATABASE_URL")
	require.NotEmpty(t, connString, "TEST_DATABASE_URL environment variable not set")

	return connString
}
