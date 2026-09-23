package namegenerator

import (
	"encoding/hex"
	"fmt"
	"io"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"

	"go.segfaultmedaddy.com/pgxephemeraltest/internal/dbmanager"
)

const (
	randomPrefixBytes = 4
	maxDatabaseBytes  = 63 // PostgreSQL's default maximum identifier length.
)

// Generator gives each factory a random prefix and each pool a distinct number.
type Generator struct {
	prefix  string
	counter atomic.Int64
}

func New(random io.Reader) (*Generator, error) {
	var data [randomPrefixBytes]byte
	if _, err := io.ReadFull(random, data[:]); err != nil {
		return nil, fmt.Errorf("reading random database prefix: %w", err)
	}

	return &Generator{prefix: hex.EncodeToString(data[:]), counter: atomic.Int64{}}, nil
}

// Generate returns a name suffix of the form <factory prefix>_<escaped test name>_<counter>.
// The test name is shortened when necessary to keep the full database name valid.
func (g *Generator) Generate(testName string) string {
	count := strconv.FormatInt(g.counter.Add(1), 10)
	escaped := url.QueryEscape(testName)

	available := maxDatabaseBytes - len(dbmanager.DatabasePrefix) - len(g.prefix) - 2 - len(count)
	if len(escaped) > available {
		escaped = escaped[:available]
		// Do not leave an incomplete percent escape at the end of the name.
		if pos := strings.LastIndexByte(escaped, '%'); pos >= 0 && len(escaped)-pos < 3 {
			escaped = escaped[:pos]
		}
	}

	return g.prefix + "_" + escaped + "_" + count
}
