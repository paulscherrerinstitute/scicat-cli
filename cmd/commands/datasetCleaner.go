package cmd

import (
	"crypto/tls"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/paulscherrerinstitute/scicat-cli/v3/cmd/cliutils"
	"github.com/paulscherrerinstitute/scicat-cli/v3/datasetUtils"
	"github.com/paulscherrerinstitute/scicat-cli/v3/orchestrator"
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

		// pass parameters
		removeFromCatalogFlag, _ := cmd.Flags().GetBool("removeFromCatalog")
		deletionCodeFlag, _ := cmd.Flags().GetString("deletionCode")
		deletionReasonFlag, _ := cmd.Flags().GetString("deletionReason")
		nonInteractiveFlag, _ := cmd.Flags().GetBool("nonInteractive")
		testenvFlag, _ := cmd.Flags().GetBool("testenv")
		devenvFlag, _ := cmd.Flags().GetBool("devenv")
		scicatUrl, _ := cmd.Flags().GetString("scicat-url")
		userpass, _ := cmd.Flags().GetString("user")
		token, _ := cmd.Flags().GetString("token")
		oidc, _ := cmd.Flags().GetBool("oidc")
		showVersion, _ := cmd.Flags().GetBool("version")

		if datasetUtils.TestFlags != nil {
			datasetUtils.TestFlags(map[string]interface{}{
				"user":              userpass,
				"token":             token,
				"testenv":           testenvFlag,
				"devenv":            devenvFlag,
				"scicat-url":        scicatUrl,
				"nonInteractive":    nonInteractiveFlag,
				"removeFromCatalog": removeFromCatalogFlag,
				"deletionCode":      deletionCodeFlag,
				"deletionReason":    deletionReasonFlag,
				"version":           showVersion,
			})
			return
		}

		// execute command
		if showVersion {
			fmt.Printf("%s\n", VERSION)
			return
		}

		// check for program version only if running interactively

		datasetUtils.CheckForNewVersion(client, CMD, VERSION)
		datasetUtils.CheckForServiceAvailability(client, testenvFlag, true)

		// configure environment
		config := cliutils.InputEnvironmentConfig{
			TestenvFlag: testenvFlag,
			DevenvFlag:  devenvFlag,
			ScicatUrl:   scicatUrl,
		}
		APIServer := config.ResolveAPIServer()

		if len(args) != 1 {
			log.Println("invalid number of args")
			return
		}
		pid := args[0]

		user, _, err := cliutils.Authenticate(cliutils.RealAuthenticator{}, client, APIServer, userpass, token, oidc)
		if err != nil {
			log.Fatal(err)
		}

		err = orchestrator.CleanDataset(client, APIServer, user, pid, nonInteractiveFlag, removeFromCatalogFlag, datasetUtils.RemovalJobOptions{
			DeletionCode:   deletionCodeFlag,
			DeletionReason: deletionReasonFlag,
		})
		if err != nil {
			log.Fatal(err)
		}
	},
}

func init() {
	rootCmd.AddCommand(datasetCleanerCmd)

	datasetCleanerCmd.Flags().Bool("removeFromCatalog", false, "Defines if the dataset should also be deleted from data catalog")
	datasetCleanerCmd.Flags().String("deletionCode", "", "Code for the deletion reason, recorded on the reset job")
	datasetCleanerCmd.Flags().String("deletionReason", "", "Reason for the deletion, recorded on the reset job")
	datasetCleanerCmd.Flags().Bool("nonInteractive", false, "Defines if no questions will be asked, just do it - make sure you know what you are doing")
	datasetCleanerCmd.Flags().Bool("testenv", false, "Use test environment (qa) instead of production environment")
	datasetCleanerCmd.Flags().Bool("devenv", false, "Use development environment instead of production environment (developers only)")

	datasetCleanerCmd.MarkFlagsMutuallyExclusive("testenv", "devenv")
}
