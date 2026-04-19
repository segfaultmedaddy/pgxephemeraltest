package cmdutil

import (
	"github.com/urfave/cli/v3"
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
