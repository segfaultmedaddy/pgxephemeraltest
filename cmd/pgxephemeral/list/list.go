package list

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
		Name:  "list",
		Usage: "List all ephemeral databases and templates",
		Flags: []cli.Flag{cmdutil.FormatFlag(), cmdutil.ConnURLFlag(), cmdutil.IncludeTemplateFlag()},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			return list(ctx, args{
				ConnURL:         cmd.String("conn-url"),
				Format:          cmd.String("format"),
				IncludeTemplate: cmd.Bool("include-template"),
			})
		},
	}
}

type args struct {
	ConnURL         string
	Format          string
	IncludeTemplate bool
}

func list(ctx context.Context, args args) error {
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
		return errors.New("no databases found")
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
