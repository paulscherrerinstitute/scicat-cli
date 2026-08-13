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

var datasetMarkForDeletionCmd = &cobra.Command{
	Use:   "datasetMarkForDeletion [options] datasetPid",
	Short: "Flag a dataset's archived data for deletion",
	Long: `Tool to flag a dataset's archived data for deletion.

If Datablock entries exist for a given dataset, a markForDeletion job will be launched, carrying
the given deletion code and reason. This does not remove anything from the archive system or the
data catalog itself - it only records the request.

Unlike datasetCleaner, this command is available to any authenticated user and never touches the
data catalog.

For further help see "` + cliutils.MANUAL + `"`,
	Args: exactArgsWithVersionException(1),
	Run: func(cmd *cobra.Command, args []string) {
		// vars & consts
		var client = &http.Client{
			Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: false}},
			Timeout:   10 * time.Second}

		const CMD = "datasetMarkForDeletion"

		// pass parameters
		deletionCodeFlag := cliutils.GetCobraStringFlag(cmd, "deletionCode")
		deletionReasonFlag := cliutils.GetCobraStringFlag(cmd, "deletionReason")
		nonInteractiveFlag := cliutils.GetCobraBoolFlag(cmd, "nonInteractive")
		testenvFlag := cliutils.GetCobraBoolFlag(cmd, "testenv")
		devenvFlag := cliutils.GetCobraBoolFlag(cmd, "devenv")
		scicatUrl := cliutils.GetCobraStringFlag(cmd, "scicat-url")
		userpass := cliutils.GetCobraStringFlag(cmd, "user")
		token := cliutils.GetCobraStringFlag(cmd, "token")
		oidc := cliutils.GetCobraBoolFlag(cmd, "oidc")
		showVersion := cliutils.GetCobraBoolFlag(cmd, "version")

		if datasetUtils.TestFlags != nil {
			datasetUtils.TestFlags(map[string]interface{}{
				"user":           userpass,
				"token":          token,
				"testenv":        testenvFlag,
				"devenv":         devenvFlag,
				"scicat-url":     scicatUrl,
				"nonInteractive": nonInteractiveFlag,
				"deletionCode":   deletionCodeFlag,
				"deletionReason": deletionReasonFlag,
				"version":        showVersion,
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

		err = orchestrator.MarkDatasetForDeletion(client, APIServer, user, pid, nonInteractiveFlag, datasetUtils.RemovalJobOptions{
			DeletionCode:   deletionCodeFlag,
			DeletionReason: deletionReasonFlag,
		})
		if err != nil {
			log.Fatal(err)
		}
	},
}

func init() {
	rootCmd.AddCommand(datasetMarkForDeletionCmd)

	datasetMarkForDeletionCmd.Flags().String("deletionCode", "", "Code for the deletion reason, recorded on the markForDeletion job")
	datasetMarkForDeletionCmd.Flags().String("deletionReason", "", "Reason for the deletion, recorded on the markForDeletion job")
	datasetMarkForDeletionCmd.Flags().Bool("nonInteractive", false, "Defines if no questions will be asked, just do it - make sure you know what you are doing")
	datasetMarkForDeletionCmd.Flags().Bool("testenv", false, "Use test environment (qa) instead of production environment")
	datasetMarkForDeletionCmd.Flags().Bool("devenv", false, "Use development environment instead of production environment (developers only)")

	datasetMarkForDeletionCmd.MarkFlagsMutuallyExclusive("testenv", "devenv")
}
