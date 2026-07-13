package cmd

import (
	"crypto/tls"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/fatih/color"
	"github.com/paulscherrerinstitute/scicat-cli/v3/cmd/cliutils"
	"github.com/paulscherrerinstitute/scicat-cli/v3/datasetIngestor"
	"github.com/paulscherrerinstitute/scicat-cli/v3/datasetUtils"
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
			LocalenvFlag: cliutils.GetCobraBoolFlag(cmd, "localenv"),
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

		if len(args) != 1 {
			log.Println("invalid number of args")
			return
		}
		pid := args[0]

		// === check for program version ===
		datasetUtils.CheckForNewVersion(client, CMD, VERSION)

		user, _, err := cliutils.Authenticate(cliutils.RealAuthenticator{}, client, APIServer, "", token, false)
		if err != nil {
			log.Fatal(err)
		}
		if user["username"] != "archiveManager" {
			log.Fatalf("You must be archiveManager to be allowed to delete datasets\n")
		}

		dataset, missing, err := datasetUtils.GetDatasetDetails(client, APIServer, user["accessToken"], []string{pid}, "")
		if err != nil {
			log.Fatal(err)
		}
		if len(missing) > 0 {
			log.Fatalf("Dataset with PID %s not found\n", pid)
		}
		if len(dataset) != 1 {
			log.Fatalf("Dataset with PID %s not found\n", pid)
		}
		if dataset[0].NumberOfFiles != 0 {
			log.Fatalf("Dataset with PID %s already contains files\n", pid)
		}

		sourceFolder := dataset[0].SourceFolder
		if sourceFolder == "" {
			log.Fatalf("Dataset with PID %s has no sourceFolder defined\n", pid)
		}

		log.Printf("Dataset with PID %s has sourceFolder %s\n", pid, sourceFolder)

		skipSymlinks := "dA"

		var skippedLinks uint = 0
		var illegalFileNames uint = 0
		localSymlinkCallback := CreateLocalSymlinkCallbackForFileLister(&skipSymlinks, &skippedLinks)
		localFilepathFilterCallback := CreateLocalFilenameFilterCallback(&illegalFileNames)

		fullFileArray, _, _, _, numFiles, totalSize, err :=
			datasetIngestor.GetLocalFileList(sourceFolder, "", localSymlinkCallback, localFilepathFilterCallback)
		if err != nil {
			log.Fatalf("Can't gather the filelist of \"%s\"", sourceFolder)
		}

		// filecount checks
		if totalSize == 0 || numFiles == 0 {
			color.Set(color.FgRed)
			log.Fatalf("\"%s\" dataset cannot be ingested - contains no files\n", sourceFolder)
			color.Unset()
		}
		if numFiles > cliutils.TOTAL_MAXFILES {
			color.Set(color.FgRed)
			log.Fatalf("\"%s\" dataset cannot be ingested - too many files: has %d, max. %d\n", sourceFolder, numFiles, cliutils.TOTAL_MAXFILES)
			color.Unset()
		}
		// print file statistics
		if skippedLinks > 0 {
			color.Set(color.FgYellow)
			log.Printf("Total number of link files skipped:%v\n", skippedLinks)
			color.Unset()
		}

		if illegalFileNames > 0 {
			color.Set(color.FgYellow)
			log.Printf("Total number of illegal file names skipped:%v\n", illegalFileNames)
			color.Unset()
		}

		err = datasetIngestor.CreateOrigDatablocks(client, APIServer, fullFileArray, pid, user)

		if err != nil {
			log.Fatalf("Error creating original data blocks for dataset %s: %v", pid, err)
		}
	},
}

func init() {
	rootCmd.AddCommand(completeIngestCmd)

	completeIngestCmd.Flags().Bool("nonInteractive", false, "Defines if no questions will be asked, just do it - make sure you know what you are doing")
	completeIngestCmd.Flags().Bool("testenv", false, "Use test environment (qa) instead of production environment")
	completeIngestCmd.Flags().Bool("devenv", false, "Use development environment instead of production environment (developers only)")

	completeIngestCmd.MarkFlagsMutuallyExclusive("testenv", "devenv")
}
