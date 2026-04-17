package migrator

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io/fs"

	"github.com/jackc/pgx/v5"
	"go.inout.gg/conduit/pkg/sqlsplit"
)

type FileMigrator struct {
	src []byte
}

func FromFile(fs fs.ReadFileFS, path string) (*FileMigrator, error) {
	src, err := fs.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read migration file %q: %w", path, err)
	}

	return &FileMigrator{src: src}, nil
}

func (m *FileMigrator) Hash() string {
	sum := sha256.Sum256(m.src)
	return hex.EncodeToString(sum[:])
}

func (m *FileMigrator) Migrate(ctx context.Context, conn *pgx.Conn) error {
	parts, err := sqlsplit.Split(m.src)
	if err != nil {
		return fmt.Errorf("split SQL migration statements: %w", err)
	}

	for _, part := range parts {
		_, err := conn.Exec(ctx, part.Content)
		if err != nil {
			return fmt.Errorf("execute SQL migration statement: %w", err)
		}
	}

	return nil
}
