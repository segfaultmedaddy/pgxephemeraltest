package pgxephemeraltest

import (
	"os"
	"testing"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)

	if code := m.Run(); code != 0 {
		os.Exit(code)
	}
}
