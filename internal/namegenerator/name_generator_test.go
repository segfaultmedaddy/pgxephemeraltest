package namegenerator_test

import (
	"bytes"
	"io"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.segfaultmedaddy.com/pgxephemeraltest/internal/dbmanager"
	"go.segfaultmedaddy.com/pgxephemeraltest/internal/namegenerator"
)

func TestGenerator(t *testing.T) {
	t.Parallel()

	g, err := namegenerator.New(bytes.NewReader([]byte{0x12, 0xab, 0x00, 0xff}))
	require.NoError(t, err)

	name := "suite/space +?💡"
	assert.Equal(t, "12ab00ff_"+url.QueryEscape(name)+"_1", g.Generate(name))
	assert.Equal(t, "12ab00ff_"+url.QueryEscape(name)+"_2", g.Generate(name))

	other, err := namegenerator.New(bytes.NewReader([]byte{0x01, 0x02, 0x03, 0x04}))
	require.NoError(t, err)
	assert.Equal(t, "01020304_"+url.QueryEscape(name)+"_1", other.Generate(name))
}

func TestGeneratorTruncatesTestName(t *testing.T) {
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
}

func TestGeneratorConcurrent(t *testing.T) {
	t.Parallel()

	g, err := namegenerator.New(bytes.NewReader([]byte{1, 2, 3, 4}))
	require.NoError(t, err)

	const count = 100

	names := make(chan string, count)

	var wg sync.WaitGroup
	for range count {
		wg.Go(func() { names <- g.Generate("test") })
	}

	wg.Wait()
	close(names)

	seen := make(map[string]bool, count)
	for name := range names {
		seen[name] = true
	}

	for i := 1; i <= count; i++ {
		assert.True(t, seen["01020304_test_"+strconv.Itoa(i)])
	}
}

func TestNewGeneratorRandomError(t *testing.T) {
	t.Parallel()

	g, err := namegenerator.New(bytes.NewReader([]byte{1}))
	assert.Nil(t, g)
	assert.ErrorIs(t, err, io.ErrUnexpectedEOF)
}
