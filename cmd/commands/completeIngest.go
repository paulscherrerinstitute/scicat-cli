package cmd

import (
	"crypto/tls"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/fatih/color"
	"github.com/paulscherrerinstitute/scicat-cli/v3/cmd/cliutils"
	"github.com/paulscherrerinstitute/scicat-cli/v3/datasetIngestor"
	"github.com/paulscherrerinstitute/scicat-cli/v3/datasetUtils"
	"github.com/paulscherrerinstitute/scicat-cli/v3/orchestrator"
	"github.com/spf13/cobra"
)

var completeIngestCmd = &cobra.Command{
	Use:   "completeIngest [options] datasetPid",
	Short: "Complete the ingestion of a dataset by adding files to an existing dataset entry in the SciCat catalog",
	Long: `Complete the ingestion of a dataset by adding files to an existing dataset entry in the SciCat catalog.
This command is used to complete the ingestion of a dataset that was previously created without
any files attached (NumberOfFiles == 0). It checks that the caller is allowed to perform the operation,
that the dataset identified by pid exists, is empty and has a sourceFolder defined, then gathers the
local file list from that sourceFolder and creates the corresponding origdatablocks. Symlinks are kept
only when they point internally to the sourceFolder; filenames containing "*", "\" or three consecutive
blanks are excluded from the dataset.

For further help see "` + cliutils.MANUAL + `"`,
	Args: rangeArgsWithVersionException(1, 2),
	Run: func(cmd *cobra.Command, args []string) {

		var client = &http.Client{
			Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: false}},
			Timeout:   120 * time.Second}

		const CMD = "completeIngest"

		// pass parameters
		envConfig := cliutils.InputEnvironmentConfig{
			TestenvFlag:  cliutils.GetCobraBoolFlag(cmd, "testenv"),
			DevenvFlag:   cliutils.GetCobraBoolFlag(cmd, "devenv"),
			ScicatUrl:    cliutils.GetCobraStringFlag(cmd, "scicat-url"),
		}

		// configure environment
		APIServer := envConfig.ResolveAPIServer()

		token := cliutils.GetCobraStringFlag(cmd, "token")
		showVersion := cliutils.GetCobraBoolFlag(cmd, "version")

		if datasetUtils.TestFlags != nil {
			datasetUtils.TestFlags(map[string]interface{}{
				"testenv":    envConfig.TestenvFlag,
				"devenv":     envConfig.DevenvFlag,
				"localenv":   envConfig.LocalenvFlag,
				"scicat-url": envConfig.ScicatUrl,
				"token":      token,
			})
			return
		}

		if showVersion {
			fmt.Printf("%s\n", VERSION)
			return
		}

		pid, err := orchestrator.ExtractPidFromArgs(args)
		if err != nil {
			log.Fatal(err)
		}

		// === check for program version ===
		datasetUtils.CheckForNewVersion(client, CMD, VERSION)

		user, _, err := cliutils.Authenticate(cliutils.RealAuthenticator{}, client, APIServer, "", token, false)
		if err != nil {
			log.Fatal(err)
		}

		err = orchestrator.CompleteIngest(client, APIServer, user, pid)
		if err != nil {
			switch err.(type) {
			case *datasetIngestor.SkippedLinksWarning, *datasetIngestor.IllegalFileNamesWarning:
				color.Set(color.FgYellow)
				log.Print(err)
				color.Unset()
			default:
				color.Set(color.FgRed)
				log.Print(err)
				color.Unset()
				os.Exit(1)
			}
		}

	},
}

func init() {
	rootCmd.AddCommand(completeIngestCmd)

	completeIngestCmd.Flags().Bool("testenv", false, "Use test environment (qa) instead of production environment")
	completeIngestCmd.Flags().Bool("devenv", false, "Use development environment instead of production environment (developers only)")

	completeIngestCmd.MarkFlagsMutuallyExclusive("testenv", "devenv")
}
