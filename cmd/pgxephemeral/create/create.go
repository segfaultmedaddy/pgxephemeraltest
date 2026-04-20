package create

import (
	"context"
	"fmt"
	"io/fs"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/urfave/cli/v3"

	"go.segfaultmedaddy.com/pgxephemeraltest/cmd/pgxephemeral/cmdutil"
	"go.segfaultmedaddy.com/pgxephemeraltest/internal/dbmanager"
	"go.segfaultmedaddy.com/pgxephemeraltest/internal/migrator"
)

func New() *cli.Command {
	//nolint:exhaustruct
	return &cli.Command{
		Name:  "create",
		Usage: "Create an ephemeral database from a template or SQL file",
		MutuallyExclusiveFlags: []cli.MutuallyExclusiveFlags{{
			//nolint:exhaustruct
			Required: true,
			Flags: [][]cli.Flag{ //nolint:exhaustruct
				{
					&cli.StringFlag{
						Name:  "from-template",
						Usage: "Name of an existing template to clone",
					}, //nolint:exhaustruct
					&cli.StringFlag{
						Name:  "from-sql",
						Usage: "Path to a SQL file to use as migration",
					}, //nolint:exhaustruct
				},
			},
		}},
		Flags: []cli.Flag{
			cmdutil.ConnURLFlag(),
			//nolint:exhaustruct
			&cli.StringFlag{Required: true, Name: "db-name", Usage: "Name for the new database"},
		},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			cwd, err := os.Getwd()
			if err != nil {
				return cmdutil.Write(nil, fmt.Errorf("get current working directory: %w", err))
			}

			//nolint:forcetypeassert // os.DirFS for real dirs implements fs.ReadFileFS.
			fsys := os.DirFS(cwd).(fs.ReadFileFS)

			return cmdutil.Write(create(ctx, fsys, args{
				ConnURL:      cmd.String("conn-url"),
				DatabaseName: cmd.String("db-name"),
				FromTemplate: cmd.String("from-template"),
				FromSQL:      cmd.String("from-sql"),
			}))
		},
	}
}

type args struct {
	ConnURL      string
	DatabaseName string
	FromTemplate string
	FromSQL      string
}

func create(ctx context.Context, fsys fs.ReadFileFS, args args) (any, error) {
	config, err := pgxpool.ParseConfig(args.ConnURL)
	if err != nil {
		return nil, fmt.Errorf("parse connection URL: %w", err)
	}

	m, err := dbmanager.New(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("create database manager: %w", err)
	}

	template := args.FromTemplate
	ret := make([]dbmanager.DBInfo, 0, 2)

	if args.FromSQL != "" {
		fileMigrator, err := migrator.FromFile(fsys, args.FromSQL)
		if err != nil {
			return nil, fmt.Errorf("load SQL migration file %q: %w", args.FromSQL, err)
		}

		template = dbmanager.TemplateName(config.ConnConfig, fileMigrator)
		if err := m.Init(ctx, fileMigrator, template); err != nil {
			return nil, fmt.Errorf("initialize template %q: %w", template, err)
		}

		ret = append(ret, dbmanager.DBInfo{Name: template, IsTemplate: true})
	}

	db, err := m.CreateDB(ctx, template, args.DatabaseName)
	if err != nil {
		return nil, fmt.Errorf("create ephemeral database from template %q: %w", template, err)
	}

	ret = append(ret, dbmanager.DBInfo{Name: db, IsTemplate: false})

	return ret, nil
}
