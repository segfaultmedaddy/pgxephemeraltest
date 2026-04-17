package cmdutil

import (
	"github.com/urfave/cli/v3"

	"go.segfaultmedaddy.com/pgxephemeraltest/cmd/pgxephemeral/viewutil"
)

func ConnURLFlag() *cli.StringFlag {
	//nolint:exhaustruct
	return &cli.StringFlag{
		Required: true,
		Name:     "conn-url",
		Usage:    "PostgreSQL connection URL",
		Sources:  cli.NewValueSourceChain(cli.EnvVar("PGXEPHEMERAL_CONN_URL")),
	}
}

func IncludeTemplateFlag() *cli.BoolFlag {
	//nolint:exhaustruct
	return &cli.BoolFlag{
		Name:  "include-template",
		Usage: "Include template databases",
	}
}

func FormatFlag() *cli.StringFlag {
	//nolint:exhaustruct
	return &cli.StringFlag{
		Name:  "format",
		Usage: "Output format (text, json)",
		Value: viewutil.FormatText,
	}
}
