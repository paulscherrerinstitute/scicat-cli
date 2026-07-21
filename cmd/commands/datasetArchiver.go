package cmd

import (
	"bufio"
	"crypto/tls"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/paulscherrerinstitute/scicat-cli/v3/cmd/cliutils"
	"github.com/paulscherrerinstitute/scicat-cli/v3/datasetUtils"
	"github.com/paulscherrerinstitute/scicat-cli/v3/orchestrator"
	"github.com/spf13/cobra"
)

var datasetArchiverCmd = &cobra.Command{
	Use:   "datasetArchiver [options] (ownerGroup | space separated list of datasetIds)",
	Short: "Archives all datasets in state datasetCreated from a given ownerGroup",
	Long: `Tool to archive datasets to the data catalog.

The ownerGroup is optional. If given, all archivable datasets of this
ownerGroup not yet archived will be archived, optionally narrowed down to
the (list of) datasetIds passed as arguments.
If ownerGroup is not given, a (list of) datasetIds must be passed as
arguments instead; all of them must exist and be archivable.

For further help see "` + cliutils.MANUAL + `"`,
	Run: func(cmd *cobra.Command, args []string) {
		// consts & vars
		var client = &http.Client{
			Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: false}},
			Timeout:   10 * time.Second}

		const CMD = "datasetArchiver"
		var scanner = bufio.NewScanner(os.Stdin)

		// pass parameters
		userpass, _ := cmd.Flags().GetString("user")
		token, _ := cmd.Flags().GetString("token")
		oidc, _ := cmd.Flags().GetBool("oidc")
		tapecopies, _ := cmd.Flags().GetInt("tapecopies")
		executionTimeStr, _ := cmd.Flags().GetString("execution-time")
		testenvFlag, _ := cmd.Flags().GetBool("testenv")
		localenvFlag, _ := cmd.Flags().GetBool("localenv")
		devenvFlag, _ := cmd.Flags().GetBool("devenv")
		scicatUrl, _ := cmd.Flags().GetString("scicat-url")
		nonInteractiveFlag, _ := cmd.Flags().GetBool("noninteractive")
		ownerGroup, _ := cmd.Flags().GetString("ownergroup")
		showVersion, _ := cmd.Flags().GetBool("version")

		if datasetUtils.TestFlags != nil {
			datasetUtils.TestFlags(map[string]interface{}{
				"user":           userpass,
				"token":          token,
				"tapecopies":     tapecopies,
				"testenv":        testenvFlag,
				"localenv":       localenvFlag,
				"devenv":         devenvFlag,
				"scicat-url":     scicatUrl,
				"noninteractive": nonInteractiveFlag,
				"version":        showVersion,
				"ownergroup":     ownerGroup,
				"execution-time": executionTimeStr})
			return
		}

		// execute command
		if showVersion {
			fmt.Printf("%s\n", VERSION)
			return
		}

		// check for program version only if running interactively
		datasetUtils.CheckForNewVersion(client, CMD, VERSION)

		// configure environment
		config := cliutils.InputEnvironmentConfig{
			TestenvFlag:  testenvFlag,
			DevenvFlag:   devenvFlag,
			LocalenvFlag: localenvFlag,
			ScicatUrl:    scicatUrl,
		}
		APIServer := config.ResolveAPIServer()

		executionTime, err := orchestrator.ParseExecutionTime(executionTimeStr)
		if err != nil {
			log.Fatal(err)
		}

		// optional list of dataset id's, if not specified, the full list of datasets of the ownergroup will be archived
		var inputdatasetList []string
		if len(ownerGroup) == 0 && len(args) == 0 {
			log.Fatalf("You must specify either an ownerGroup or a list of datasetIds to archive")
		}

		if len(args) > 0 {
			inputdatasetList = args[0:]
		}

		user, accessGroups, err := cliutils.Authenticate(cliutils.RealAuthenticator{}, client, APIServer, userpass, token, oidc)
		if err != nil {
			log.Fatal(err)
		}

		resolvedOwnerGroup, err := orchestrator.ResolveOwnerGroup(ownerGroup, accessGroups)
		if err != nil {
			log.Fatal(err)
		}
		archivableDatasets, err := orchestrator.ResolveArchivableDatasets(client, APIServer, user["accessToken"], resolvedOwnerGroup, inputdatasetList)
		if err != nil {
			log.Fatal(err)
		}

		archive := ""
		if nonInteractiveFlag {
			archive = "y"
		} else {
			fmt.Printf("\nDo you want to archive these %v datasets (y/N) ? ", len(archivableDatasets))
			scanner.Scan()
			archive = scanner.Text()
		}

		if archive != "y" {
			log.Fatalf("Okay the archive process is stopped here, no datasets will be archived\n")
		}

		log.Printf("You chose to archive the new datasets\n")
		log.Printf("Submitting Archive Job for the ingested datasets.\n")
		jobId, err := datasetUtils.CreateArchivalJob(client, APIServer, user, resolvedOwnerGroup, archivableDatasets, &tapecopies, executionTime)
		if err != nil {
			log.Fatalf("Couldn't create a job: %s\n", err.Error())
		}
		fmt.Println(jobId)
	},
}

func init() {
	rootCmd.AddCommand(datasetArchiverCmd)

	datasetArchiverCmd.Flags().Int("tapecopies", 1, "Number of tapecopies to be used for archiving")
	datasetArchiverCmd.Flags().String("execution-time", "", "The time when the command should be executed in RFC3339 format")
	datasetArchiverCmd.Flags().Bool("testenv", false, "Use test environment (qa) instead or production")
	datasetArchiverCmd.Flags().Bool("localenv", false, "Use local environment (local) instead or production")
	datasetArchiverCmd.Flags().Bool("devenv", false, "Use development environment instead or production")
	datasetArchiverCmd.Flags().Bool("noninteractive", false, "Defines if no questions will be asked, just do it - make sure you know what you are doing")
	datasetArchiverCmd.Flags().String("ownergroup", "", "Specifies to which owner group should the archival job belong. If no dataset id's are passed, all datasets belonging to this ownergroup that can also be marked as archivable will be included. If not specified, a list of dataset id's must be passed as arguments instead")

	datasetArchiverCmd.MarkFlagsMutuallyExclusive("testenv", "localenv", "devenv")
}
