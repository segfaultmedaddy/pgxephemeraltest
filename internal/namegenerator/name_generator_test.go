package namegenerator_test

import (
	"bytes"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.segfaultmedaddy.com/pgxephemeraltest/internal/dbmanager"
	"go.segfaultmedaddy.com/pgxephemeraltest/internal/namegenerator"
)

func TestGenerator(t *testing.T) {
	t.Parallel()

	t.Run("it escapes test names", func(t *testing.T) {
		t.Parallel()

		g, err := namegenerator.New(bytes.NewReader([]byte{0x12, 0xab, 0x00, 0xff}))
		require.NoError(t, err)

		name := "suite/space +?💡"
		assert.Equal(t, "12ab00ff_suite%2Fspace+%2B%3F%F0%9F%92%A1_1", g.Generate(name))
		assert.Equal(t, "12ab00ff_suite%2Fspace+%2B%3F%F0%9F%92%A1_2", g.Generate(name))
	})

	t.Run("it truncates long test names", func(t *testing.T) {
		t.Parallel()

		g, err := namegenerator.New(bytes.NewReader([]byte{0, 0, 0, 0}))
		require.NoError(t, err)

		for i, tc := range []struct {
			name     string
			expected string
		}{
			{strings.Repeat("a", 100), strings.Repeat("a", 45)},
			{strings.Repeat("a", 44) + "/", strings.Repeat("a", 44)},
			{strings.Repeat("a", 43) + "/", strings.Repeat("a", 43)},
		} {
			suffix := g.Generate(tc.name)
			assert.Equal(t, "00000000_"+tc.expected+"_"+strconv.Itoa(i+1), suffix)
			assert.LessOrEqual(t, len(dbmanager.DatabasePrefix+suffix), 63)
		}

		for range 6 {
			g.Generate("other")
		}

		suffix := g.Generate(strings.Repeat("a", 100))
		assert.Equal(t, "00000000_"+strings.Repeat("a", 44)+"_10", suffix)
		assert.Len(t, dbmanager.DatabasePrefix+suffix, 63)
	})
}
