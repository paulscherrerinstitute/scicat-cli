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

var datasetRemoverCmd = &cobra.Command{
	Use:   "datasetRemover [options] datasetPid",
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

		const CMD = "datasetRemover"

		// pass parameters
		nonInteractiveFlag, _ := cmd.Flags().GetBool("nonInteractive")
		testenvFlag, _ := cmd.Flags().GetBool("testenv")
		devenvFlag, _ := cmd.Flags().GetBool("devenv")
		scicatUrl, _ := cmd.Flags().GetString("scicat-url")
		userpass, _ := cmd.Flags().GetString("user")
		token, _ := cmd.Flags().GetString("token")
		oidc, _ := cmd.Flags().GetBool("oidc")
		showVersion, _ := cmd.Flags().GetBool("version")
		deletionCode, _ := cmd.Flags().GetString("deletionCode")
		deletionReason, _ := cmd.Flags().GetString("deletionReason")

		if datasetUtils.TestFlags != nil {
			datasetUtils.TestFlags(map[string]interface{}{
				"user":           userpass,
				"token":          token,
				"testenv":        testenvFlag,
				"devenv":         devenvFlag,
				"scicat-url":     scicatUrl,
				"nonInteractive": nonInteractiveFlag,
				"version":        showVersion,
				"deletionCode":   deletionCode,
				"deletionReason": deletionReason,
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
		APIServer := cliutils.ConfigureEnvironment(cliutils.InputEnvironmentConfig{
			TestenvFlag: testenvFlag,
			DevenvFlag:  devenvFlag,
			ScicatUrl:   scicatUrl,
		})

		if len(args) != 1 {
			log.Println("invalid number of args")
			return
		}
		pid := args[0]

		user, _, err := cliutils.Authenticate(cliutils.RealAuthenticator{}, client, APIServer, userpass, token, oidc)
		if err != nil {
			log.Fatal(err)
		}

		jobID, err := datasetUtils.RemoveFromArchive(client, APIServer, pid, user, nonInteractiveFlag, datasetUtils.JobParamsStruct{
			DeletionCode:   datasetUtils.DeletionCode(deletionCode),
			DeletionReason: deletionReason,
		})
		if err != nil {
			if jobID != "" {
				patchError := datasetUtils.PatchJobStatus(client, APIServer, user, jobID, string(datasetUtils.JobFailed))
				if patchError != nil {
					log.Fatalf("Failed to patch job status: %v", patchError)
				}
			}
			log.Fatal(err)
		}
	},
}

func init() {
	rootCmd.AddCommand(datasetRemoverCmd)

	datasetRemoverCmd.Flags().Bool("removeFromCatalog", false, "Defines if the dataset should also be deleted from data catalog")
	datasetRemoverCmd.Flags().Bool("nonInteractive", false, "Defines if no questions will be asked, just do it - make sure you know what you are doing")
	datasetRemoverCmd.Flags().Bool("testenv", false, "Use test environment (qa) instead of production environment")
	datasetRemoverCmd.Flags().Bool("devenv", false, "Use development environment instead of production environment (developers only)")
	datasetRemoverCmd.Flags().String("deletionCode", "", "Code for the deletion reason")
	datasetRemoverCmd.Flags().String("deletionReason", "", "Reason for the deletion")
	datasetRemoverCmd.MarkFlagRequired("deletionCode")
	datasetRemoverCmd.MarkFlagsMutuallyExclusive("testenv", "devenv")
}
