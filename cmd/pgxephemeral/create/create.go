package create

import (
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/urfave/cli/v3"

	"go.segfaultmedaddy.com/pgxephemeraltest/cmd/pgxephemeral/cmdutil"
	"go.segfaultmedaddy.com/pgxephemeraltest/cmd/pgxephemeral/viewutil"
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
			cmdutil.FormatFlag(),
			cmdutil.ConnURLFlag(),
			//nolint:exhaustruct
			&cli.StringFlag{Required: true, Name: "db-name", Usage: "Name for the new database"},
		},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			cwd, err := os.Getwd()
			if err != nil {
				return fmt.Errorf("get current working directory: %w", err)
			}

			//nolint:forcetypeassert // os.DirFS for real dirs implements fs.ReadFileFS.
			fsys := os.DirFS(cwd).(fs.ReadFileFS)

			return create(ctx, fsys, args{
				ConnURL:      cmd.String("conn-url"),
				Format:       cmd.String("format"),
				DatabaseName: cmd.String("db-name"),
				FromTemplate: cmd.String("from-template"),
				FromSQL:      cmd.String("from-sql"),
			})
		},
	}
}

type args struct {
	Format       string
	ConnURL      string
	DatabaseName string
	FromTemplate string
	FromSQL      string
}

func create(ctx context.Context, fsys fs.ReadFileFS, args args) error {
	config, err := pgxpool.ParseConfig(args.ConnURL)
	if err != nil {
		return fmt.Errorf("parse connection URL: %w", err)
	}

	m, err := dbmanager.New(ctx, config)
	if err != nil {
		return fmt.Errorf("create database manager: %w", err)
	}

	template := args.FromTemplate
	if args.FromSQL != "" {
		fileMigrator, err := migrator.FromFile(fsys, args.FromSQL)
		if err != nil {
			return fmt.Errorf("load SQL migration file %q: %w", args.FromSQL, err)
		}

		template = dbmanager.TemplateName(config.ConnConfig, fileMigrator)
		if err := m.Init(ctx, fileMigrator, template); err != nil {
			return fmt.Errorf("initialize template %q: %w", template, err)
		}
	}

	db, err := m.CreateDB(ctx, template, args.DatabaseName)
	if err != nil {
		return fmt.Errorf("create ephemeral database from template %q: %w", template, err)
	}

	switch args.Format {
	case viewutil.FormatText:
		if _, err := fmt.Fprintf(
			os.Stdout,
			"created a `%s` database using `%s` template\n",
			db,
			template,
		); err != nil {
			return fmt.Errorf("write text output: %w", err)
		}
	case viewutil.FormatJSON:
		enc := json.NewEncoder(os.Stdout)
		if err := enc.Encode(map[string]string{"db": db, "template": template}); err != nil {
			return fmt.Errorf("write JSON output: %w", err)
		}

	default:
		return fmt.Errorf("unknown format: %s", args.Format)
	}

	return nil
}
