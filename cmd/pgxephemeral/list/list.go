package list

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/urfave/cli/v3"

	"go.segfaultmedaddy.com/pgxephemeraltest/cmd/pgxephemeral/cmdutil"
	"go.segfaultmedaddy.com/pgxephemeraltest/internal/dbmanager"
	"go.segfaultmedaddy.com/pgxephemeraltest/internal/sliceutil"
)

func New() *cli.Command {
	//nolint:exhaustruct
	return &cli.Command{
		Name:  "list",
		Usage: "List all ephemeral databases and templates",
		Flags: []cli.Flag{cmdutil.ConnURLFlag(), cmdutil.IncludeTemplateFlag()},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			return cmdutil.Write(list(ctx, args{
				ConnURL:         cmd.String("conn-url"),
				IncludeTemplate: cmd.Bool("include-template"),
			}))
		},
	}
}

type args struct {
	ConnURL         string
	IncludeTemplate bool
}

func list(ctx context.Context, args args) (any, error) {
	config, err := pgxpool.ParseConfig(args.ConnURL)
	if err != nil {
		return nil, fmt.Errorf("parse connection URL: %w", err)
	}

	m, err := dbmanager.New(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("create database manager: %w", err)
	}

	dbs, err := m.ListDBs(ctx)
	if err != nil {
		return nil, fmt.Errorf("list ephemeral databases: %w", err)
	}

	if !args.IncludeTemplate {
		dbs = sliceutil.Filter(dbs, func(db dbmanager.DBInfo) bool {
			return !db.IsTemplate
		})
	}

	if len(dbs) == 0 {
		return nil, errors.New("no databases found")
	}

	return dbs, nil
}
