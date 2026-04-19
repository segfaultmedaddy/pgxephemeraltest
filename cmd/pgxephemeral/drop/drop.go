package drop

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"slices"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/urfave/cli/v3"

	"go.segfaultmedaddy.com/pgxephemeraltest/cmd/pgxephemeral/cmdutil"
	"go.segfaultmedaddy.com/pgxephemeraltest/internal/dbmanager"
	"go.segfaultmedaddy.com/pgxephemeraltest/internal/sliceutil"
)

func New() *cli.Command {
	//nolint:exhaustruct
	return &cli.Command{
		Name:  "drop",
		Usage: "Drop ephemeral databases",
		Flags: []cli.Flag{cmdutil.ConnURLFlag(), cmdutil.IncludeTemplateFlag()},
		MutuallyExclusiveFlags: []cli.MutuallyExclusiveFlags{{
			//nolint:exhaustruct
			Required: true,
			Flags: [][]cli.Flag{ //nolint:exhaustruct
				{
					&cli.StringSliceFlag{
						Name:  "db-name",
						Usage: "Name of the database to drop (repeatable)",
					}, //nolint:exhaustruct
					&cli.BoolFlag{
						Name:  "all",
						Usage: "Drop all ephemeral databases",
					}, //nolint:exhaustruct
				},
			},
		}},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			return drop(ctx, args{
				ConnURL:         cmd.String("conn-url"),
				DatabaseNames:   cmd.StringSlice("db-name"),
				All:             cmd.Bool("all"),
				IncludeTemplate: cmd.Bool("include-template"),
			})
		},
	}
}

type args struct {
	ConnURL         string
	DatabaseNames   []string
	All             bool
	IncludeTemplate bool
}

func drop(ctx context.Context, args args) error {
	config, err := pgxpool.ParseConfig(args.ConnURL)
	if err != nil {
		return fmt.Errorf("parse connection URL: %w", err)
	}

	m, err := dbmanager.New(ctx, config)
	if err != nil {
		return fmt.Errorf("create database manager: %w", err)
	}

	knownDBs, err := m.ListDBs(ctx)
	if err != nil {
		return fmt.Errorf("list ephemeral databases: %w", err)
	}

	if !args.IncludeTemplate {
		knownDBs = sliceutil.Filter(knownDBs, func(db dbmanager.DBInfo) bool {
			return !db.IsTemplate
		})
	}

	if len(knownDBs) == 0 {
		return errors.New("no databases to drop")
	}

	toDrop := make(map[string]dbmanager.DBInfo, len(knownDBs))
	for _, db := range knownDBs {
		toDrop[db.Name] = db
	}

	if !args.All {
		if len(args.DatabaseNames) == 0 {
			return errors.New("at least one --db-name must be provided")
		}

		for name := range toDrop {
			if !slices.Contains(args.DatabaseNames, name) {
				delete(toDrop, name)
			}
		}

		if len(toDrop) == 0 {
			return errors.New("no matching databases to drop")
		}
	}

	names := slices.Collect(maps.Keys(toDrop))

	if err := m.DropDBs(ctx, names); err != nil {
		if args.All {
			return fmt.Errorf("drop all selected databases: %w", err)
		}

		return fmt.Errorf("drop selected databases: %w", err)
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")

	if err := enc.Encode(slices.Collect(maps.Values(toDrop))); err != nil {
		return fmt.Errorf("write JSON output: %w", err)
	}

	return nil
}
