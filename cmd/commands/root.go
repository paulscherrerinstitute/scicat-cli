package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "cmd",
	Short: "CLI app for interacting with a SciCat instance",
	Long: `This library comprises a few subcommands for managing SciCat
and datasets on it, as well as interacting with the archival system connected
to it.`,
	Run: func(cmd *cobra.Command, args []string) {
		version, _ := cmd.Flags().GetBool("version")
		if version {
			fmt.Printf("%s\n", VERSION)
			return
		}
		fmt.Print("No action was specified.\n\n")
		cmd.Help()
	},
}

func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {
	rootCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")

	rootCmd.PersistentFlags().StringP("user", "u", "", "Defines optional username:password string")
	rootCmd.PersistentFlags().String("token", "", "Defines optional API token instead of username:password")
	rootCmd.PersistentFlags().StringP("config", "c", "", "A path to a config file for connecting to SciCat and transfer services")
	rootCmd.PersistentFlags().StringP("scicat-url", "s", "", "The scicat url to use. Note: it'll overwrite any built-in environments.")
	rootCmd.PersistentFlags().Bool("oidc", false, "Use OIDC for login instead of internal user")
	rootCmd.PersistentFlags().BoolP("version", "v", false, "Show version")

	rootCmd.MarkFlagsMutuallyExclusive("user", "token", "oidc")
}
