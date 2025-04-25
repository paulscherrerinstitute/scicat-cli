package cmd

import (
	"crypto/tls"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/fatih/color"
	"github.com/paulscherrerinstitute/scicat-cli/v3/datasetUtils"
	"github.com/spf13/cobra"
)

var datasetRetrieverCmd = &cobra.Command{
	Use:   "datasetRetriever (options) local-destination-path",
	Short: "Retrieve datasets from intermediate cache, taking into account original sourceFolder names",
	Long: `Tool to retrieve datasets from the intermediate cache server of the tape archive to the 
destination path on your local system.

This script must be run on the machine having write access to the destination folder

The resulting files from dataset folders will be stores in destinationPath/sourceFolders

In case there are several datasets with the same sourceFolder they will be simply enumerated by appending a "_1", "_2" etc. (not yet implemenmted)

Per default all available datasets on the retrieve server will be fetched.\n")
Use option -dataset or -ownerGroup to restrict the datasets which should be fetched.

For further help see "` + MANUAL + `"`,
	Args: exactArgsWithVersionException(1),
	Run: func(cmd *cobra.Command, args []string) {
		//consts & vars
		const PROD_RSYNC_RETRIEVE_SERVER string = "pb-retrieve.psi.ch"
		const TEST_RSYNC_RETRIEVE_SERVER string = "pbt-retrieve.psi.ch"
		const DEV_RSYNC_RETRIEVE_SERVER string = "arematest2in.psi.ch"
		const LOCAL_RSYNC_RETRIEVE_SERVER string = "localhost"

		// const PROD_RSYNC_RETRIEVE_SERVER string = "ebarema4in.psi.ch"
		// const TEST_RSYNC_RETRIEVE_SERVER string = "ebaremat1in.psi.ch"
		// const DEV_RSYNC_RETRIEVE_SERVER string = "arematest2in.psi.ch"

		// TODO Windows
		const APP = "datasetRetriever"

		var APIServer string = PROD_API_SERVER
		var RSYNCServer string = PROD_RSYNC_RETRIEVE_SERVER
		var env string = "production"

		var client = &http.Client{
			Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: false}},
			Timeout:   10 * time.Second}

		// internal functions
		assembleRsyncCommands := func(username string, datasetDetails []datasetUtils.Dataset, destinationPath string) ([]string, []string) {
			batchCommands := make([]string, len(datasetDetails))
			destinationFolders := make([]string, len(datasetDetails))
			for i, dataset := range datasetDetails {
				shortDatasetId := strings.Split(dataset.Pid, "/")[1]
				fullDest := destinationPath + dataset.SourceFolder
				command := "mkdir -p " + fullDest + ";" + "/usr/bin/rsync -av -e 'ssh -o StrictHostKeyChecking=no' " + username + "@" + RSYNCServer + ":retrieve/" + shortDatasetId + "/ " + fullDest
				batchCommands[i] = command
				destinationFolders[i] = fullDest
			}
			return batchCommands, destinationFolders
		}

		executeCommands := func(batchCommands []string) {
			log.Printf("\n\n\n====== Starting transfer of dataset files: \n\n")
			for _, batchCommand := range batchCommands {
				cmd := exec.Command("/bin/sh", "-c", batchCommand)
				cmd.Stdout = os.Stdout
				cmd.Stderr = os.Stderr
				//log.Printf("Running %v.\n", cmd.Args)
				log.Printf("\n=== Transfer command: %s.\n", batchCommand)

				err := cmd.Run()

				if err != nil {
					log.Fatal(err)
				}
			}
		}

		checkSumVerification := func(destinationFolders []string) {
			// sed '/is_directory$/d' __checksum_filename_*__ |  awk -v FS='    ' '/^[^#]/{print $2,$1}' | sha1sum -c
			log.Printf("\n\n\n====== Starting verification of check sums: \n\n")
			for _, destination := range destinationFolders {
				command := "cd " + destination + " ; sed '/is_directory$/d' __checksum_filename_*__ |  awk -v FS='    ' '/^[^#]/{print $2,$1}' | sha1sum -c"
				cmd := exec.Command("/bin/sh", "-c", command)
				cmd.Stdout = os.Stdout
				cmd.Stderr = os.Stderr
				// log.Printf("Running %v.\n", cmd.Args)
				log.Printf("\n=== Checking files within %s.\n", destination)
				err := cmd.Run()

				if err != nil {
					log.Fatal(err)
				}
			}
		}

		// retrieve flags
		// TODO (from orig. code) extract jobId and checksum flags
		retrieveFlag, _ := cmd.Flags().GetBool("retrieve")
		userpass, _ := cmd.Flags().GetString("user")
		token, _ := cmd.Flags().GetString("token")
		oidc, _ := cmd.Flags().GetBool("oidc")
		nochksumFlag, _ := cmd.Flags().GetBool("nochksum")
		datasetId, _ := cmd.Flags().GetString("dataset")
		ownerGroup, _ := cmd.Flags().GetString("ownergroup")
		testenvFlag, _ := cmd.Flags().GetBool("testenv")
		devenvFlag, _ := cmd.Flags().GetBool("devenv")
		scicatUrl, _ := cmd.Flags().GetString("scicat-url")
		localenvFlag, _ := cmd.Flags().GetBool("localenv")
		showVersion, _ := cmd.Flags().GetBool("version")

		if datasetUtils.TestFlags != nil {
			datasetUtils.TestFlags(map[string]interface{}{
				"retrieve":   retrieveFlag,
				"testenv":    testenvFlag,
				"devenv":     devenvFlag,
				"user":       userpass,
				"token":      token,
				"nochksum":   nochksumFlag,
				"dataset":    datasetId,
				"ownergroup": ownerGroup,
				"version":    showVersion,
			})
			return
		}

		// execute command
		if showVersion {
			fmt.Printf("%s\n", VERSION)
			return
		}

		datasetUtils.CheckForNewVersion(client, APP, VERSION)

		if localenvFlag {
			APIServer = LOCAL_API_SERVER
			RSYNCServer = LOCAL_RSYNC_RETRIEVE_SERVER
		}
		if devenvFlag {
			APIServer = DEV_API_SERVER
			RSYNCServer = DEV_RSYNC_RETRIEVE_SERVER
			env = "dev"
		}
		if testenvFlag {
			APIServer = TEST_API_SERVER
			RSYNCServer = TEST_RSYNC_RETRIEVE_SERVER
			env = "test"
		}
		if scicatUrl != "" {
			APIServer = scicatUrl
			env = "custom"
		}

		color.Set(color.FgGreen)
		log.Printf("You are about to retrieve dataset(s) from the === %s === retrieve server...", env)
		color.Unset()

		if !retrieveFlag {
			color.Set(color.FgRed)
			log.Printf("Note: you run in 'dry' mode to simply check which data would be fetched.\n")
			log.Printf("Use the -retrieve flag to actually transfer the datasets to your chosen destination path.\n")
			color.Unset() // Don't forget to unset
		}

		destinationPath := ""

		if len(args) != 1 {
			log.Fatalln("invalid number of args")
		}
		destinationPath = args[0]

		user, _, err := authenticate(RealAuthenticator{}, client, APIServer, userpass, token, oidc)
		if err != nil {
			log.Fatal(err)
		}

		datasetList, err := datasetUtils.GetAvailableDatasets(user["username"], RSYNCServer, datasetId)
		if err != nil {
			log.Fatal(err)
		}

		if len(datasetList) == 0 {
			log.Printf("\n\nNo datasets found on intermediate cache server.\n")
			log.Fatalln("Did you submit a retrieve job from the data catalog first ?")
		}

		// get sourceFolder and other dataset related info for all Datasets
		datasetDetails, missingDatasetIds, err := datasetUtils.GetDatasetDetails(client, APIServer, user["accessToken"], datasetList, ownerGroup)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("\nFound datasets:\n")
		fmt.Println("Dataset ID                                         Size[MB]  Owner                      SourceFolder")
		fmt.Println("====================================================================================================")
		for _, datasetDetail := range datasetDetails {
			fmt.Printf("%s %9d %v %v\n", datasetId, datasetDetail.Size/1024./1024., datasetDetail.OwnerGroup, datasetDetail.SourceFolder)
		}
		if len(missingDatasetIds) > 0 {
			fmt.Printf("\nThe following dataset id's were missing or had non-matching ownerGroups so they won't be copied: \n")
			for _, id := range missingDatasetIds {
				fmt.Printf(" - \"%s\"\n", id)
			}
		}

		// assemble rsync commands to be submitted
		batchCommands, destinationFolders := assembleRsyncCommands(user["username"], datasetDetails, destinationPath)
		// log.Printf("%v\n", batchCommands)

		if !retrieveFlag {
			color.Set(color.FgRed)
			log.Printf("\n\nNote: you run in 'dry' mode to simply check what would happen.")
			log.Printf("Use the -retrieve flag to actually retrieve datasets.")
			color.Unset()
		} else {
			executeCommands(batchCommands)
			if !nochksumFlag {
				checkSumVerification(destinationFolders)
			}
		}
	},
}

func init() {
	rootCmd.AddCommand(datasetRetrieverCmd)

	datasetRetrieverCmd.Flags().Bool("retrieve", false, "Defines if this command is meant to actually copy data to the local system (default nothing is done)")
	datasetRetrieverCmd.Flags().Bool("nochksum", false, "Switch off chksum verification step (default checksum tests are done)")
	datasetRetrieverCmd.Flags().String("dataset", "", "Defines single dataset to retrieve (default all available datasets)")
	datasetRetrieverCmd.Flags().String("ownergroup", "", "Defines to fetch only datasets of the specified ownerGroup (default is to fetch all available datasets)")
	datasetRetrieverCmd.Flags().Bool("testenv", false, "Use test environment (qa) (default is to use production system)")
	datasetRetrieverCmd.Flags().Bool("devenv", false, "Use development environment (default is to use production system)")
	datasetRetrieverCmd.Flags().Bool("localenv", false, "Use local environment instead of production environment (developers only)")

	datasetRetrieverCmd.MarkFlagsMutuallyExclusive("testenv", "devenv", "scicat-url")
}
