/*

Purpose: Remove dataset from archive and optionally from data catalog

This script must be run by the archiveManager role.

If Datablock entries exist for a given dataset a reset job will be launched
If the Dataset should be removed from the data catalog as well the corresponding
documents in Dataset and OrigDatablock will be deleted as well. This will only
happen once the reset job is finished. The tool will try to remove the dataset
catalog entries each minute until Dataset is found to be in archivable statet again
and only then will be deleted in the data catalog

Note: these actions can not be un-done  ! Be careful

Call like this:

datasetCleaner --removeFromCatalog datasetPid

   - Return code (useful for wrapper scripts):
	 rc=0: command excuted correctly
	 rc=1: command exited with errors and needs to be repeated


*/

package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"log"
	"net/http"
	"github.com/paulscherrerinstitute/scicat/datasetUtils"
	"time"

	"github.com/fatih/color"
)

func isFlagPassed(name string) bool {
	found := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == name {
			found = true
		}
	})
	return found
}

var VERSION string

func main() {
	var client = &http.Client{
		Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: false}},
		Timeout:   10 * time.Second}

	const PROD_API_SERVER string = "https://dacat.psi.ch/api/v3"
	const TEST_API_SERVER string = "https://dacat-qa.psi.ch/api/v3"
	const DEV_API_SERVER string = "https://dacat-development.psi.ch/api/v3"

	const MANUAL string = "http://melanie.gitpages.psi.ch/SciCatPages"
	const APP = "datasetCleaner"

	var APIServer string
	var env string

	// pass parameters
	removeFromCatalogFlag := flag.Bool("removeFromCatalog", false, "Defines if the dataset should also be deleted from data catalog")
	nonInteractiveFlag := flag.Bool("nonInteractive", false, "Defines if no questions will be asked, just do it - make sure you know what you are doing")
	testenvFlag := flag.Bool("testenv", false, "Use test environment (qa) instead of production environment")
	devenvFlag := flag.Bool("devenv", false, "Use development environment instead of production environment (developers only)")
	userpass := flag.String("user", "", "Defines optional username:password string")
	token := flag.String("token", "", "Defines optional API token instead of username:password")
	showVersion := flag.Bool("version", false, "Show version number and exit")

	flag.Parse()
	
	if *showVersion {
		fmt.Printf("%s\n", VERSION)
		return
	}
	
	// check for program version only if running interactively
	datasetUtils.CheckForNewVersion(client, APP, VERSION)
	datasetUtils.CheckForServiceAvailability(client, *testenvFlag, true)

	//}

	if *testenvFlag {
		APIServer = TEST_API_SERVER
		env = "test"
	} else if *devenvFlag {
		APIServer = DEV_API_SERVER
		env = "dev"
	} else {
		APIServer = PROD_API_SERVER
		env = "production"
	}

	color.Set(color.FgRed)
	log.Printf("You are about to remove a dataset from the === %s === data catalog environment...", env)
	color.Unset()

	args := flag.Args()
	pid := ""

	if len(args) == 1 {
		pid = args[0]
	} else {
		fmt.Printf("\n\nTool to remove datasets from the data catalog.\n\n")
		fmt.Printf("Run script with one dataset pid as argument:\n\n")
		fmt.Printf("datasetIngestor [options] dataset-PID\n\n")
		flag.PrintDefaults()
		fmt.Printf("\n\nFor further help see " + MANUAL + "\n\n")
		return
	}

	auth := &datasetUtils.RealAuthenticator{}
	user, _ := datasetUtils.Authenticate(auth, client, APIServer, token, userpass)

	if user["username"] != "archiveManager" {
		log.Fatalf("You must be archiveManager to be allowed to delete datasets\n")
	}

	datasetUtils.RemoveFromArchive(client, APIServer, pid, user, *nonInteractiveFlag)

	if *removeFromCatalogFlag {
		datasetUtils.RemoveFromCatalog(client, APIServer, pid, user, *nonInteractiveFlag)
	} else {
		log.Println("To also delete the dataset from the catalog add the flag -removeFromCatalog")
	}
}
