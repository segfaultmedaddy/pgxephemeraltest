package drop

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"

	tea "charm.land/bubbletea/v2"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/urfave/cli/v3"

	"go.segfaultmedaddy.com/pgxephemeraltest/cmd/pgxephemeral/cmdutil"
	"go.segfaultmedaddy.com/pgxephemeraltest/cmd/pgxephemeral/viewutil"
	"go.segfaultmedaddy.com/pgxephemeraltest/internal/dbmanager"
	"go.segfaultmedaddy.com/pgxephemeraltest/internal/sliceutil"
)

func New() *cli.Command {
	//nolint:exhaustruct
	return &cli.Command{
		Name:  "drop",
		Usage: "Drop ephemeral databases",
		Flags: []cli.Flag{cmdutil.FormatFlag(), cmdutil.ConnURLFlag(), cmdutil.IncludeTemplateFlag()},
		MutuallyExclusiveFlags: []cli.MutuallyExclusiveFlags{{
			//nolint:exhaustruct
			Required: true,
			Flags: [][]cli.Flag{ //nolint:exhaustruct
				{
					&cli.StringFlag{
						Name:  "db-name",
						Usage: "Name of the database to drop",
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
				DatabaseName:    cmd.String("db-name"),
				All:             cmd.Bool("all"),
				Format:          cmd.String("format"),
				IncludeTemplate: cmd.Bool("include-template"),
			})
		},
	}
}

type args struct {
	ConnURL         string
	DatabaseName    string
	Format          string
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

	dbs, err := m.ListDBs(ctx)
	if err != nil {
		return fmt.Errorf("list ephemeral databases: %w", err)
	}

	if !args.IncludeTemplate {
		dbs = sliceutil.Filter(dbs, func(db dbmanager.DBInfo) bool {
			return !db.IsTemplate
		})
	}

	if len(dbs) == 0 {
		return errors.New("no databases to drop")
	}

	if args.All {
		names := make([]string, 0, len(dbs))
		for _, db := range dbs {
			names = append(names, db.Name)
		}

		if err := m.DropDBs(ctx, names); err != nil {
			return fmt.Errorf("drop all selected databases: %w", err)
		}
	} else {
		dbs = sliceutil.Filter(dbs, func(db dbmanager.DBInfo) bool {
			return db.Name == args.DatabaseName
		})

		if len(dbs) == 0 {
			return fmt.Errorf("database %s not found", args.DatabaseName)
		}

		if err := m.DropDB(ctx, args.DatabaseName, args.IncludeTemplate); err != nil {
			return fmt.Errorf("drop database %q: %w", args.DatabaseName, err)
		}
	}

	switch args.Format {
	case viewutil.FormatText:
		{
			p := tea.NewProgram(viewutil.NewTableView(dbs))
			if _, err := p.Run(); err != nil {
				return fmt.Errorf("render table view: %w", err)
			}
		}

	case viewutil.FormatJSON:
		{
			enc := json.NewEncoder(os.Stdout)
			enc.SetIndent("", "  ")

			if err := enc.Encode(dbs); err != nil {
				return fmt.Errorf("write JSON output: %w", err)
			}
		}

	default:
		return fmt.Errorf("unknown format: %s", args.Format)
	}

	return nil
}
