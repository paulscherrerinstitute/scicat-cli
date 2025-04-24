package cmd

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
)

func exactArgsWithVersionException(n int) cobra.PositionalArgs {
	return func(cmd *cobra.Command, args []string) error {
		version, _ := cmd.Flags().GetBool("version")
		if version {
			return nil
		}
		if len(args) != n {
			return fmt.Errorf("accepts %d arg(s), received %d", n, len(args))
		}
		return nil
	}
}

func minArgsWithVersionException(n int) cobra.PositionalArgs {
	return func(cmd *cobra.Command, args []string) error {
		version, _ := cmd.Flags().GetBool("version")
		if version {
			return nil
		}
		if len(args) < n {
			return fmt.Errorf("requires at least %d arg(s), only received %d", n, len(args))
		}
		return nil
	}
}

func rangeArgsWithVersionException(min int, max int) cobra.PositionalArgs {
	return func(cmd *cobra.Command, args []string) error {
		version, _ := cmd.Flags().GetBool("version")
		if version {
			return nil
		}
		if len(args) < min || len(args) > max {
			return fmt.Errorf("accepts between %d and %d arg(s), received %d", min, max, len(args))
		}
		return nil
	}
}

// Expands ~ to the user's home directory
func expandPath(path string) string {
	if len(path) == 0 ||
		path[0] != '~' ||
		(len(path) > 1 && path[1] != '/' && path[1] != '\\') {
		return path
	}
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return path
	}
	return filepath.Join(homeDir, path[1:])
}
