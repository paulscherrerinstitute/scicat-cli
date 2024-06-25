package cmd

import (
	"fmt"

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
