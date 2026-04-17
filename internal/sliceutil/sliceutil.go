package sliceutil

// Map returns a new slice containing the results of applying f to each element of s.
func Map[T any, U any](s []T, f func(T) U) []U {
	ret := make([]U, len(s))
	for i, v := range s {
		ret[i] = f(v)
	}

	return ret
}

// Filter returns a new slice containing only the elements of s that satisfy f.
func Filter[S ~[]T, T any](s S, f func(T) bool) S {
	ret := make([]T, 0)

	for _, v := range s {
		if f(v) {
			ret = append(ret, v)
		}
	}

	return ret
}
