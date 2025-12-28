package internaltesting

import "context"

// TB is the interface common to testing.T, testing.B, and testing.F.
//
// It copied from the testing package.
//
//nolint:interfacebloat // copied from testing package.
//go:generate mockgen -destination=mock_testing.go -package internaltesting go.segfaultmedaddy.com/pgxephemeraltest/internal/internaltesting TB
type TB interface {
	Cleanup(func())
	Error(args ...any)
	Errorf(format string, args ...any)
	Fail()
	FailNow()
	Failed() bool
	Fatal(args ...any)
	Fatalf(format string, args ...any)
	Helper()
	Log(args ...any)
	Logf(format string, args ...any)
	Name() string
	Setenv(key, value string)
	Chdir(dir string)
	Skip(args ...any)
	SkipNow()
	Skipf(format string, args ...any)
	Skipped() bool
	TempDir() string
	Context() context.Context
}
