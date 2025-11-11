package sharedpool

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPool(t *testing.T) {
	t.Parallel()

	for i := range 10 {
		t.Run(fmt.Sprintf("test_%d", i), func(t *testing.T) {
			t.Parallel()

			var (
				ctx = t.Context()
				p   = Pool(t)
			)

			_, err := p.Exec(
				ctx,
				`insert into kv ("key", "value") values ($1, $2)`,
				"key",
				fmt.Sprintf("value_%d", i),
			)
			require.NoError(t, err)

			rows, err := p.Query(ctx, "select * from kv")
			require.NoError(t, err)

			defer rows.Close()

			for rows.Next() {
				var key, value string

				err = rows.Scan(&key, &value)
				require.NoError(t, err)
				require.Equal(t, "key", key)
				require.Equal(t, fmt.Sprintf("value_%d", i), value)
			}

			require.NoError(t, rows.Err())
		})
	}
}
