package cmd

import (
	"bufio"
	"crypto/tls"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/fatih/color"
	"github.com/paulscherrerinstitute/scicat-cli/v3/datasetUtils"
	"github.com/spf13/cobra"
)

var datasetArchiverCmd = &cobra.Command{
	Aliases: []string{"a", "archive"},
	Use:     "datasetArchiver [options] (ownerGroup | space separated list of datasetIds)",
	Short:   "Archives all datasets in state datasetCreated from a given ownerGroup",
	Long: `Tool to archive datasets to the data catalog.

You must choose either an ownerGroup, in which case all archivable datasets
of this ownerGroup not yet archived will be archived.
Or you choose a (list of) datasetIds, in which case all archivable datasets
of this list not yet archived will be archived.

For further help see "` + MANUAL + `"`,
	Run: func(cmd *cobra.Command, args []string) {
		// consts & vars
		var client = &http.Client{
			Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: false}},
			Timeout:   10 * time.Second}

		const CMD = "datasetArchiver"
		var scanner = bufio.NewScanner(os.Stdin)

		var APIServer string = PROD_API_SERVER
		var env string = "production"

		// pass parameters
		userpass, _ := cmd.Flags().GetString("user")
		token, _ := cmd.Flags().GetString("token")
		oidc, _ := cmd.Flags().GetBool("oidc")
		tapecopies, _ := cmd.Flags().GetInt("tapecopies")
		executionTimeStr, _ := cmd.Flags().GetString("executionTime")
		testenvFlag, _ := cmd.Flags().GetBool("testenv")
		localenvFlag, _ := cmd.Flags().GetBool("localenv")
		devenvFlag, _ := cmd.Flags().GetBool("devenv")
		scicatUrl, _ := cmd.Flags().GetString("scicat-url")
		nonInteractiveFlag, _ := cmd.Flags().GetBool("noninteractive")
		ownergroupFlag, _ := cmd.Flags().GetString("ownergroup")
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
				"ownergroup":     ownergroupFlag,
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

		if localenvFlag {
			APIServer = LOCAL_API_SERVER
			env = "local"
		}
		if devenvFlag {
			APIServer = DEV_API_SERVER
			env = "dev"
		}
		if testenvFlag {
			APIServer = TEST_API_SERVER
			env = "test"
		}
		if scicatUrl != "" {
			APIServer = scicatUrl
			env = "custom"
		}

		var executionTime *time.Time = nil
		if executionTimeStr != "" {
			parsedTime, err := time.Parse(time.RFC3339, executionTimeStr)
			if err != nil {
				log.Fatalf("Execution time is invalid: %s", err.Error())
			}
			executionTime = &parsedTime
		}

		color.Set(color.FgGreen)
		log.Printf("You are about to archive dataset(s) to the === %s === data catalog environment...", env)
		color.Unset()

		ownerGroup := ownergroupFlag

		// optional list of dataset id's, if not specified, the full list of datasets of the ownergroup will be archived
		var inputdatasetList []string
		if len(args) > 0 {
			inputdatasetList = args[0:]
		}

		user, _, err := authenticate(RealAuthenticator{}, client, APIServer, userpass, token, oidc)
		if err != nil {
			log.Fatal(err)
		}

		archivableDatasets, err := datasetUtils.GetArchivableDatasets(client, APIServer, ownerGroup, inputdatasetList, user["accessToken"])
		if err != nil {
			log.Fatalf("GetArchivableDatasets: %s\n", err.Error())
		}
		if len(archivableDatasets) <= 0 {
			log.Fatalln("No archivable datasets remaining")
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
		jobId, err := datasetUtils.CreateArchivalJob(client, APIServer, user, ownerGroup, archivableDatasets, &tapecopies, executionTime)
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
	datasetArchiverCmd.Flags().String("ownergroup", "", "Specifies to which owner group should the archival job belong. If no datasets id's are passed, all datasets belonging to this ownergroup that can also be marked as archivable will be included")

	datasetArchiverCmd.MarkFlagsMutuallyExclusive("testenv", "localenv", "devenv")
	datasetArchiverCmd.MarkFlagRequired("ownergroup")
}
