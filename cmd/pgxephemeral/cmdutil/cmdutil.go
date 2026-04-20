package cmdutil

import (
	"encoding/json"
	"fmt"
	"os"

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

// Write writes the given value to stdout or stderr, depending on the error.
func Write(v any, err error) error {
	if err != nil {
		if err := json.NewEncoder(os.Stderr).Encode(map[string]string{"error": err.Error()}); err != nil {
			return fmt.Errorf("write error output: %w", err)
		}

		return err
	}

	if v != nil {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")

		if err := enc.Encode(map[string]any{"result": v}); err != nil {
			return fmt.Errorf("write result output: %w", err)
		}
	}

	return nil
}
