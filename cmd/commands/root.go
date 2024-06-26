package cmd

import (
	"os"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "cmd",
	Short: "CLI app for interacting with a SciCat instance",
	Long: `This library comprises a few subcommands for managing SciCat
and datasets on it, as well as interacting with the archival system connected
to it.`,
	// uncomment the next line if there's a default action
	// Run: func(cmd *cobra.Command, args []string) { },
}

func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {
	rootCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
