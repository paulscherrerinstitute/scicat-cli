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
		ingestorConfig := cliutils.IngestorConfig{
			Userpass:       cliutils.GetCobraStringFlag(cmd, "user"),
			Token:          cliutils.GetCobraStringFlag(cmd, "token"),
			ScicatUrl:      cliutils.GetCobraStringFlag(cmd, "scicat-url"),
			Testenv:        cliutils.GetCobraBoolFlag(cmd, "testenv"),
			Devenv:         cliutils.GetCobraBoolFlag(cmd, "devenv"),
			Oidc:           cliutils.GetCobraBoolFlag(cmd, "oidc"),
			NonInteractive: cliutils.GetCobraBoolFlag(cmd, "nonInteractive"),
			DeletionCode:   cliutils.GetCobraStringFlag(cmd, "deletionCode"),
			DeletionReason: cliutils.GetCobraStringFlag(cmd, "deletionReason"),
		}

		showVersion := cliutils.GetCobraBoolFlag(cmd, "version")

		if datasetUtils.TestFlags != nil {
			datasetUtils.TestFlags(map[string]interface{}{
				"user":           ingestorConfig.Userpass,
				"token":          ingestorConfig.Token,
				"testenv":        ingestorConfig.Testenv,
				"devenv":         ingestorConfig.Devenv,
				"scicat-url":     ingestorConfig.ScicatUrl,
				"nonInteractive": ingestorConfig.NonInteractive,
				"version":        showVersion,
				"deletionCode":   ingestorConfig.DeletionCode,
				"deletionReason": ingestorConfig.DeletionReason,
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
		ingestorConfig.PID = args[0]

		err := cliutils.RunDeletion(client, ingestorConfig, VERSION, CMD)
		if err != nil {
			log.Fatal(err)
		}
	},
}

func init() {
	rootCmd.AddCommand(datasetRemoverCmd)

	datasetRemoverCmd.Flags().Bool("nonInteractive", false, "Defines if no questions will be asked, just do it - make sure you know what you are doing")
	datasetRemoverCmd.Flags().Bool("testenv", false, "Use test environment (qa) instead of production environment")
	datasetRemoverCmd.Flags().Bool("devenv", false, "Use development environment instead of production environment (developers only)")
	datasetRemoverCmd.Flags().String("deletionCode", "", "Code for the deletion reason")
	datasetRemoverCmd.Flags().String("deletionReason", "", "Reason for the deletion")
	datasetRemoverCmd.MarkFlagRequired("deletionCode")
	datasetRemoverCmd.MarkFlagsMutuallyExclusive("testenv", "devenv")
}
