package main

import (
	"context"
	"fmt"
	"os"

	"github.com/urfave/cli/v3"

	"go.segfaultmedaddy.com/pgxephemeraltest/cmd/pgxephemeral/create"
	"go.segfaultmedaddy.com/pgxephemeraltest/cmd/pgxephemeral/drop"
	"go.segfaultmedaddy.com/pgxephemeraltest/cmd/pgxephemeral/list"
)

func main() {
	if err := run(context.Background()); err != nil {
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	//nolint:exhaustruct
	app := cli.Command{
		Name:     "pgxephemeral",
		Usage:    "Manage ephemeral PostgreSQL databases for testing",
		Commands: []*cli.Command{create.New(), drop.New(), list.New()},
	}

	if err := app.Run(ctx, os.Args); err != nil {
		return fmt.Errorf("run CLI app: %w", err)
	}

	return nil
}
