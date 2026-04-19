package sliceutil

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMap(t *testing.T) {
	t.Parallel()

	s := []int{1, 2, 3, 4, 5}
	ret := Map(s, func(i int) int { return i * 2 })
	require.Equal(t, []int{2, 4, 6, 8, 10}, ret)
	require.Len(t, ret, len(s))
}

func TestFilter(t *testing.T) {
	t.Parallel()

	s := []int{1, 2, 3, 4, 5}
	ret := Filter(s, func(i int) bool { return i%2 == 0 })
	require.Equal(t, []int{2, 4}, ret)
}

func TestUniq(t *testing.T) {
	t.Parallel()

	s := []int{1, 2, 3, 4, 5, 1, 2, 3, 4, 5}
	ret := Uniq(s)
	require.ElementsMatch(t, []int{1, 2, 3, 4, 5}, ret)
}
