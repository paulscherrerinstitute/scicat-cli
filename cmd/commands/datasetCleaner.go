package cmd

import (
	"crypto/tls"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/fatih/color"
	"github.com/paulscherrerinstitute/scicat/datasetUtils"
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

For further help see "` + MANUAL + `"`,
	Args: exactArgsWithVersionException(1),
	Run: func(cmd *cobra.Command, args []string) {
		// vars & consts
		var client = &http.Client{
			Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: false}},
			Timeout:   10 * time.Second}

		const CMD = "datasetCleaner"

		var APIServer string = PROD_API_SERVER
		var env string = "production"

		// pass parameters
		removeFromCatalogFlag, _ := cmd.Flags().GetBool("removeFromCatalog")
		nonInteractiveFlag, _ := cmd.Flags().GetBool("nonInteractive")
		testenvFlag, _ := cmd.Flags().GetBool("testenv")
		devenvFlag, _ := cmd.Flags().GetBool("devenv")
		userpass, _ := cmd.Flags().GetString("user")
		token, _ := cmd.Flags().GetString("token")
		showVersion, _ := cmd.Flags().GetBool("version")

		if datasetUtils.TestFlags != nil {
			datasetUtils.TestFlags(map[string]interface{}{
				"user":              userpass,
				"token":             token,
				"testenv":           testenvFlag,
				"devenv":            devenvFlag,
				"nonInteractive":    nonInteractiveFlag,
				"removeFromCatalog": removeFromCatalogFlag,
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

		//}

		if devenvFlag {
			APIServer = DEV_API_SERVER
			env = "dev"
		}
		if testenvFlag {
			APIServer = TEST_API_SERVER
			env = "test"
		}

		color.Set(color.FgRed)
		log.Printf("You are about to remove a dataset from the === %s === data catalog environment...", env)
		color.Unset()

		if len(args) != 1 {
			log.Println("invalid number of args")
			return
		}
		pid := args[0]

		user, _ := authenticate(RealAuthenticator{}, client, APIServer, userpass, token)

		if user["username"] != "archiveManager" {
			log.Fatalf("You must be archiveManager to be allowed to delete datasets\n")
		}

		datasetUtils.RemoveFromArchive(client, APIServer, pid, user, nonInteractiveFlag)

		if removeFromCatalogFlag {
			datasetUtils.RemoveFromCatalog(client, APIServer, pid, user, nonInteractiveFlag)
		} else {
			log.Println("To also delete the dataset from the catalog add the flag -removeFromCatalog")
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
