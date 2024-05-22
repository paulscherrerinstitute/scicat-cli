/*

Purpose: retrieve datasets from intermediate cache, taking into account original sourceFolder names

This script must be run on the machine having write access to the destination folder

The resulting files from dataset folders will be stores in destinationPath/sourceFolders

In case there are several datasets with the same sourceFolder they will be simply enumerated by appending a "_1", "_2" etc. (not yet implemenmted)

*/

package main

import (
	"bufio"
	"crypto/tls"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"github.com/paulscherrerinstitute/scicat/datasetUtils"
	"strings"
	"time"

	"github.com/fatih/color"
)

const PROD_API_SERVER string = "https://dacat.psi.ch/api/v3"
const TEST_API_SERVER string = "https://dacat-qa.psi.ch/api/v3"
const DEV_API_SERVER string = "https://dacat-development.psi.ch/api/v3"

const PROD_RSYNC_RETRIEVE_SERVER string = "pb-retrieve.psi.ch"
const TEST_RSYNC_RETRIEVE_SERVER string = "pbt-retrieve.psi.ch"
const DEV_RSYNC_RETRIEVE_SERVER string = "arematest2in.psi.ch"

// const PROD_RSYNC_RETRIEVE_SERVER string = "ebarema4in.psi.ch"
// const TEST_RSYNC_RETRIEVE_SERVER string = "ebaremat1in.psi.ch"
// const DEV_RSYNC_RETRIEVE_SERVER string = "arematest2in.psi.ch"

const MANUAL string = "http://melanie.gitpages.psi.ch/SciCatPages/#sec-5"

// TODO Windows
const APP = "datasetRetriever"

var APIServer string = PROD_API_SERVER
var RSYNCServer string = PROD_RSYNC_RETRIEVE_SERVER

var client = &http.Client{
	Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: false}},
	Timeout:   10 * time.Second}
var scanner = bufio.NewScanner(os.Stdin)
var VERSION string

func main() {
	// check input parameters

	retrieveFlag := flag.Bool("retrieve", false, "Defines if this command is meant to actually copy data to the local system (default nothing is done)")
	userpass := flag.String("user", "", "Defines optional username and password (default is to prompt for username and password)")
	token := flag.String("token", "", "Defines optional API token instead of username:password")
	nochksumFlag := flag.Bool("nochksum", false, "Switch off chksum verification step (default checksum tests are done)")
	datasetId := flag.String("dataset", "", "Defines single dataset to retrieve (default all available datasets)")
	ownerGroup := flag.String("ownergroup", "", "Defines to fetch only datasets of the specified ownerGroup (default is to fetch all available datasets)")
	testenvFlag := flag.Bool("testenv", false, "Use test environment (qa) (default is to use production system)")
	devenvFlag := flag.Bool("devenv", false, "Use development environment (default is to use production system)")
	showVersion := flag.Bool("version", false, "Show version number and exit")

	flag.Parse()

	if *showVersion {
		fmt.Printf("%s\n", VERSION)
		return
	}
	
	datasetUtils.CheckForNewVersion(client, APP, VERSION)

	var env string
	if *testenvFlag {
		APIServer = TEST_API_SERVER
		RSYNCServer = TEST_RSYNC_RETRIEVE_SERVER
		env = "test"
	} else if *devenvFlag {
		APIServer = DEV_API_SERVER
		RSYNCServer = DEV_RSYNC_RETRIEVE_SERVER
		env = "dev"
	} else {
		APIServer = PROD_API_SERVER
		RSYNCServer = PROD_RSYNC_RETRIEVE_SERVER
		env = "production"
	}

	color.Set(color.FgGreen)
	log.Printf("You are about to retrieve dataset(s) from the === %s === retrieve server...", env)
	color.Unset()

	if !*retrieveFlag {
		color.Set(color.FgRed)
		log.Printf("Note: you run in 'dry' mode to simply check which data would be fetched.\n")
		log.Printf("Use the -retrieve flag to actually transfer the datasets to your chosen destination path.\n")
		color.Unset() // Don't forget to unset
	}

	// TODO extract jobId and checksum flags
	args := flag.Args()
	destinationPath := ""

	if len(args) == 1 {
		destinationPath = args[0]
	} else {
		fmt.Println("\n\nTool to retrieve datasets from the intermediate cache server of the tape archive")
		fmt.Printf("to the destination path on your local system.\n")
		fmt.Printf("Run script with 1 argument:\n\n")
		fmt.Printf("datasetRetriever [options] local-destination-path\n\n")
		fmt.Printf("Per default all available datasets on the retrieve server will be fetched.\n")
		fmt.Printf("Use option -dataset or -ownerGroup to restrict the datasets which should be fetched.\n\n")
		flag.PrintDefaults()
		fmt.Printf("\n\nFor further help see " + MANUAL + "\n\n")
		return
	}

	auth := &datasetUtils.RealAuthenticator{}
	user, _ := datasetUtils.Authenticate(auth, client, APIServer, token, userpass)

	datasetList, err := datasetUtils.GetAvailableDatasets(user["username"], RSYNCServer, *datasetId)
	if err != nil {
		log.Fatal(err)
	}

	if len(datasetList) == 0 {
		fmt.Printf("\n\nNo datasets found on intermediate cache server.\n")
		fmt.Println("Did you submit a retrieve job from the data catalog first ?")
	} else {
		// get sourceFolder and other dataset related info for all Datasets
		datasetDetails := datasetUtils.GetDatasetDetails(client, APIServer, user["accessToken"], datasetList, *ownerGroup)

		// assemble rsync commands to be submitted
		batchCommands, destinationFolders := assembleRsyncCommands(user["username"], datasetDetails, destinationPath)
		// log.Printf("%v\n", batchCommands)

		if !*retrieveFlag {
			color.Set(color.FgRed)
			log.Printf("\n\nNote: you run in 'dry' mode to simply check what would happen.")
			log.Printf("Use the -retrieve flag to actually retrieve datasets.")
			color.Unset()
		} else {
			executeCommands(batchCommands)
			if !*nochksumFlag {
				checkSumVerification(destinationFolders)
			}
		}
	}
}

type Dataset struct {
	Pid           string
	SourceFolder  string
	Size          int
	OwnerGroup    string
	NumberOfFiles int
}

// TODO handle case where several datasets have same sourceFolders

func assembleRsyncCommands(username string, datasetDetails []datasetUtils.Dataset, destinationPath string) ([]string, []string) {
	batchCommands := make([]string, 0)
	destinationFolders := make([]string, 0)
	for _, dataset := range datasetDetails {
		shortDatasetId := strings.Split(dataset.Pid, "/")[1]
		fullDest := destinationPath + dataset.SourceFolder
		command := "mkdir -p " + fullDest + ";" + "/usr/bin/rsync -av -e 'ssh -o StrictHostKeyChecking=no' " + username + "@" + RSYNCServer + ":retrieve/" + shortDatasetId + "/ " + fullDest
		batchCommands = append(batchCommands, command)
		destinationFolders = append(destinationFolders, fullDest)
	}
	return batchCommands, destinationFolders
}

func executeCommands(batchCommands []string) {
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

func checkSumVerification(destinationFolders []string) {
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
