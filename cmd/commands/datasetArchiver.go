package cmd

import (
	"bufio"
	"crypto/tls"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/fatih/color"
	"github.com/paulscherrerinstitute/scicat/datasetUtils"
	"github.com/spf13/cobra"
)

var datasetArchiverCmd = &cobra.Command{
	Use:   "datasetArchiver [options] (ownerGroup | space separated list of datasetIds)",
	Short: "Archives all datasets in state datasetCreated from a given ownerGroup",
	Long: `Tool to archive datasets to the data catalog.

You must choose either an ownerGroup, in which case all archivable datasets
of this ownerGroup not yet archived will be archived.
Or you choose a (list of) datasetIds, in which case all archivable datasets
of this list not yet archived will be archived. 

For further help see "` + MANUAL + `"`,
	Args: minArgsWithVersionException(1),
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
		tapecopies, _ := cmd.Flags().GetInt("tapecopies")
		testenvFlag, _ := cmd.Flags().GetBool("testenv")
		localenvFlag, _ := cmd.Flags().GetBool("localenv")
		devenvFlag, _ := cmd.Flags().GetBool("devenv")
		nonInteractiveFlag, _ := cmd.Flags().GetBool("noninteractive")
		showVersion, _ := cmd.Flags().GetBool("version")

		if datasetUtils.TestFlags != nil {
			datasetUtils.TestFlags(map[string]interface{}{
				"user":           userpass,
				"token":          token,
				"tapecopies":     tapecopies,
				"testenv":        testenvFlag,
				"localenv":       localenvFlag,
				"devenv":         devenvFlag,
				"noninteractive": nonInteractiveFlag,
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

		color.Set(color.FgGreen)
		log.Printf("You are about to archive dataset(s) to the === %s === data catalog environment...", env)
		color.Unset()

		ownerGroup := ""
		inputdatasetList := make([]string, 0)

		// argsWithoutProg := os.Args[1:]
		if len(args) <= 0 {
			log.Fatalln("invalid number of args")
		}

		if len(args) == 1 && !strings.Contains(args[0], "/") {
			ownerGroup = args[0]
		} else {
			inputdatasetList = args[0:]
		}

		user, _ := authenticate(RealAuthenticator{}, client, APIServer, userpass, token)

		archivableDatasets := datasetUtils.GetArchivableDatasets(client, APIServer, ownerGroup, inputdatasetList, user["accessToken"])
		if len(archivableDatasets) <= 0 {
			log.Fatalf("No archivable datasets remaining")
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
		jobId, err := datasetUtils.CreateArchivalJob(client, APIServer, user, archivableDatasets, &tapecopies)
		if err != nil {
			log.Fatalf("Couldn't create a job: %s\n", err.Error())
		}
		fmt.Println(jobId)
	},
}

func init() {
	rootCmd.AddCommand(datasetArchiverCmd)

	datasetArchiverCmd.Flags().Int("tapecopies", 1, "Number of tapecopies to be used for archiving")
	datasetArchiverCmd.Flags().Bool("testenv", false, "Use test environment (qa) instead or production")
	datasetArchiverCmd.Flags().Bool("localenv", false, "Use local environment (local) instead or production")
	datasetArchiverCmd.Flags().Bool("devenv", false, "Use development environment instead or production")
	datasetArchiverCmd.Flags().Bool("noninteractive", false, "Defines if no questions will be asked, just do it - make sure you know what you are doing")

	datasetArchiverCmd.MarkFlagsMutuallyExclusive("testenv", "localenv", "devenv")
}
