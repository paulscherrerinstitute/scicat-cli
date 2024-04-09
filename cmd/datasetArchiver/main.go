/*

This script archives all datasets in state datasetCreated from a given ownerGroup

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
	"github.com/paulscherrerinstitute/scicat/datasetUtils"
	"strings"
	"time"

	"github.com/fatih/color"
)

var VERSION string

func main() {
	var client = &http.Client{
		Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: false}},
		Timeout:   10 * time.Second}

	const PROD_API_SERVER string = "https://dacat.psi.ch/api/v3"
	const TEST_API_SERVER string = "https://dacat-qa.psi.ch/api/v3"
	const DEV_API_SERVER string = "https://dacat-development.psi.ch/api/v3"
	const LOCAL_API_SERVER string = "http://localhost:3000/api/v3"

	const MANUAL string = "http://melanie.gitpages.psi.ch/SciCatPages"
	const APP = "datasetArchiver"
	var scanner = bufio.NewScanner(os.Stdin)

	var APIServer string
	var env string

	// pass parameters
	userpass := flag.String("user", "", "Defines optional username and password")
	token := flag.String("token", "", "Defines optional API token instead of username:password")
	tapecopies := flag.Int("tapecopies", 1, "Number of tapecopies to be used for archiving")
	testenvFlag := flag.Bool("testenv", false, "Use test environment (qa) instead or production")
	localenvFlag := flag.Bool("localenv", false, "Use local environment (local) instead or production")
	devenvFlag := flag.Bool("devenv", false, "Use development environment instead or production")
	nonInteractiveFlag := flag.Bool("noninteractive", false, "Defines if no questions will be asked, just do it - make sure you know what you are doing")
	showVersion := flag.Bool("version", false, "Show version number and exit")

	flag.Parse()
	
	if *showVersion {
		fmt.Printf("%s\n", VERSION)
		return
	}

	// check for program version only if running interactively
	datasetUtils.CheckForNewVersion(client, APP, VERSION)

	if *testenvFlag {
		APIServer = TEST_API_SERVER
		env = "test"
	} else if *devenvFlag {
		APIServer = DEV_API_SERVER
		env = "dev"
	} else if *localenvFlag {
		APIServer = LOCAL_API_SERVER
		env = "local"
	} else {
		APIServer = PROD_API_SERVER
		env = "production"
	}

	color.Set(color.FgGreen)
	log.Printf("You are about to archive dataset(s) to the === %s === data catalog environment...", env)
	color.Unset()

	args := flag.Args()
	ownerGroup := ""
	inputdatasetList := make([]string, 0)

	// argsWithoutProg := os.Args[1:]
	if len(args) == 0 {
		fmt.Printf("\n\nTool to archive datasets to the data catalog.\n\n")
		fmt.Printf("Run script with the following options and parameter:\n\n")
		fmt.Printf("datasetArchiver [options] (ownerGroup | space separated list of datasetIds) \n\n")
		fmt.Printf("You must choose either an ownerGroup, in which case all archivable datasets\n")
		fmt.Printf("of this ownerGroup not yet archived will be archived.\n")
		fmt.Printf("Or you choose a (list of) datasetIds, in which case all archivable datasets\n")
		fmt.Printf("of this list not yet archived will be archived.\n\n")
		fmt.Printf("List of options:\n\n")
		flag.PrintDefaults()
		fmt.Printf("\n\nFor further help see " + MANUAL + "\n\n")
		return
	} else if len(args) == 1 && !strings.Contains(args[0], "/") {
		ownerGroup = args[0]
	} else {
		inputdatasetList = args[0:]
	}

	user, _ := datasetUtils.Authenticate(client, APIServer, token, userpass)

	archivableDatasets := datasetUtils.GetArchivableDatasets(client, APIServer, ownerGroup, inputdatasetList, user["accessToken"])
	if len(archivableDatasets) > 0 {
		archive := ""
		if *nonInteractiveFlag {
			archive = "y"
		} else {
			fmt.Printf("\nDo you want to archive these %v datasets (y/N) ? ", len(archivableDatasets))
			scanner.Scan()
			archive = scanner.Text()
		}
		if archive != "y" {
			log.Fatalf("Okay the archive process is stopped here, no datasets will be archived\n")
		} else {
			log.Printf("You chose to archive the new datasets\n")
			log.Printf("Submitting Archive Job for the ingested datasets.\n")
			jobId := datasetUtils.CreateJob(client, APIServer, user, archivableDatasets, tapecopies)
			fmt.Println(jobId)
		}
	} else {
		log.Fatalf("No archivable datasets remaining")
	}
}
