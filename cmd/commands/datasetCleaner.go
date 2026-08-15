package cmd

import (
	"crypto/tls"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/paulscherrerinstitute/scicat-cli/v3/cmd/cliutils"
	"github.com/paulscherrerinstitute/scicat-cli/v3/datasetUtils"
	"github.com/spf13/cobra"
)

var datasetCleanerCmd = &cobra.Command{
	Use:   "datasetCleaner [options] datasetPid",
	Short: "Remove dataset from archive and optionally from data catalog",
	Long: `Tool to remove datasets from the data catalog.
	
If Datablock entries exist for a given dataset, a reset job will be launched.

If the Dataset should be removed from the data catalog, the corresponding
documents in Dataset and OrigDatablock will be deleted as well. This will only
happen once the reset job is finished. The tool will try to remove the dataset
catalog entries each minute until Dataset is found to be in archivable state again,
and only then it will be deleted in the data catalog.

Note: these actions can not be un-done! Be careful!

For further help see "` + cliutils.MANUAL + `"`,
	Args: exactArgsWithVersionException(1),
	Run: func(cmd *cobra.Command, args []string) {
		// vars & consts
		var client = &http.Client{
			Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: false}},
			Timeout:   10 * time.Second}

		const CMD = "datasetCleaner"

		envConfig := cliutils.InputEnvironmentConfig{
			TestenvFlag: cliutils.GetCobraBoolFlag(cmd, "testenv"),
			DevenvFlag:  cliutils.GetCobraBoolFlag(cmd, "devenv"),
			ScicatUrl:   cliutils.GetCobraStringFlag(cmd, "scicat-url"),
		}

		// pass parameters
		ingestorConfig := cliutils.CleanConfig{
			BaseConfig: cliutils.BaseConfig{
				Userpass:       cliutils.GetCobraStringFlag(cmd, "user"),
				Token:          cliutils.GetCobraStringFlag(cmd, "token"),
				EnvConfig:      envConfig,
				Oidc:           cliutils.GetCobraBoolFlag(cmd, "oidc"),
				NonInteractive: cliutils.GetCobraBoolFlag(cmd, "nonInteractive"),
				HttpClient:     client,
			},
			RemoveFromCatalog: cliutils.GetCobraBoolFlag(cmd, "removeFromCatalog"),
		}

		showVersion := cliutils.GetCobraBoolFlag(cmd, "version")

		if datasetUtils.TestFlags != nil {
			datasetUtils.TestFlags(map[string]interface{}{
				"user":              ingestorConfig.Userpass,
				"token":             ingestorConfig.Token,
				"testenv":           envConfig.TestenvFlag,
				"devenv":            envConfig.DevenvFlag,
				"scicat-url":        envConfig.ScicatUrl,
				"nonInteractive":    ingestorConfig.NonInteractive,
				"removeFromCatalog": ingestorConfig.RemoveFromCatalog,
				"version":           showVersion,
			})
			return
		}

		// execute command
		if showVersion {
			fmt.Printf("%s\n", VERSION)
			return
		}

		if len(args) != 1 {
			log.Println("invalid number of args")
			return
		}

		err := ingestorConfig.RunFullRemoval(args[0], VERSION, CMD)
		if err != nil {
			log.Fatal(err)
		}

		if !ingestorConfig.RemoveFromCatalog {
			log.Println("To also delete the dataset from the catalog add the flag --removeFromCatalog")
		}
	},
}

func init() {
	rootCmd.AddCommand(datasetCleanerCmd)

	datasetCleanerCmd.Flags().Bool("removeFromCatalog", false, "Defines if the dataset should also be deleted from data catalog")
	datasetCleanerCmd.Flags().Bool("nonInteractive", false, "Defines if no questions will be asked, just do it - make sure you know what you are doing")
	datasetCleanerCmd.Flags().Bool("testenv", false, "Use test environment (qa) instead of production environment")
	datasetCleanerCmd.Flags().Bool("devenv", false, "Use development environment instead of production environment (developers only)")

	datasetCleanerCmd.MarkFlagsMutuallyExclusive("testenv", "devenv")
}
