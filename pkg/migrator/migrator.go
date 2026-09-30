// Package migrator provides filesystem-backed SQL migrations for pgxephemeraltest.
package migrator

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io/fs"
	"path"
	"slices"

	"github.com/jackc/pgx/v5"

	"go.segfaultmedaddy.com/pgxephemeraltest/v2"
	"go.segfaultmedaddy.com/pgxephemeraltest/v2/pkg/sqlsplit"
)

var _ pgxephemeraltest.Migrator = (*FSMigrator)(nil)

// FSMigrator applies a snapshot of SQL migration files in lexicographic path order.
type FSMigrator struct {
	hash       string
	migrations []migration
}

type migration struct {
	path string
	src  []byte
}

// FromFS recursively loads regular .sql files from fsys, starting at its root.
// Files are applied in lexicographic order of their full paths. Use fs.Sub to
// select a migration directory within a larger filesystem.
//
// Files are read once, so Hash and Migrate always use the same snapshot.
func FromFS(fsys fs.FS) (*FSMigrator, error) {
	var paths []string

	err := fs.WalkDir(fsys, ".", func(name string, entry fs.DirEntry, err error) error {
		if err != nil {
			return fmt.Errorf("walk migration path %q: %w", name, err)
		}

		if entry.Type().IsRegular() && path.Ext(name) == ".sql" {
			paths = append(paths, name)
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("walk migration filesystem: %w", err)
	}

	slices.Sort(paths)

	migrations := make([]migration, 0, len(paths))

	for _, name := range paths {
		src, err := fs.ReadFile(fsys, name)
		if err != nil {
			return nil, fmt.Errorf("read migration file %q: %w", name, err)
		}

		migrations = append(migrations, migration{path: name, src: src})
	}

	return newFSMigrator(migrations), nil
}

// FromFile loads one SQL migration file from fsys.
func FromFile(fsys fs.FS, name string) (*FSMigrator, error) {
	src, err := fs.ReadFile(fsys, name)
	if err != nil {
		return nil, fmt.Errorf("read migration file %q: %w", name, err)
	}

	return newFSMigrator([]migration{{path: name, src: src}}), nil
}

func newFSMigrator(migrations []migration) *FSMigrator {
	digest := sha256.New()

	for _, migration := range migrations {
		// Length-prefix both fields to distinguish file and content boundaries.
		fmt.Fprintf(digest, "%d:%s%d:", len(migration.path), migration.path, len(migration.src))
		digest.Write(migration.src)
	}

	return &FSMigrator{migrations: migrations, hash: hex.EncodeToString(digest.Sum(nil))}
}

// Hash identifies the ordered migration paths and contents.
func (m *FSMigrator) Hash() string { return m.hash }

// Migrate applies each file's SQL statements in order and stops at the first error.
func (m *FSMigrator) Migrate(ctx context.Context, conn *pgx.Conn) error {
	for _, migration := range m.migrations {
		parts, err := sqlsplit.Split(migration.src)
		if err != nil {
			return fmt.Errorf("split SQL migration %q: %w", migration.path, err)
		}

		for _, part := range parts {
			if part.Type != sqlsplit.StmtTypeQuery {
				continue
			}

			if _, err := conn.Exec(ctx, part.Content); err != nil {
				return fmt.Errorf(
					"execute SQL migration %q at %s: %w",
					migration.path,
					part.Start,
					err,
				)
			}
		}
	}

	return nil
}
