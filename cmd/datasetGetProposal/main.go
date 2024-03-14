/*

This script returns the proposal information for a given ownerGroup

*/

package main

import (
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"github.com/paulscherrerinstitute/scicat/datasetUtils"
	"time"

	"github.com/fatih/color"
)

func main() {

	var client = &http.Client{
		Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: false}},
		Timeout:   10 * time.Second}

	const PROD_API_SERVER string = "https://dacat.psi.ch/api/v3"
	const TEST_API_SERVER string = "https://dacat-qa.psi.ch/api/v3"
	const DEV_API_SERVER string = "https://dacat-development.psi.ch/api/v3"

	const MANUAL string = "http://melanie.gitpages.psi.ch/SciCatPages"
	const APP = "datasetGetProposal"
	const VERSION = "1.1.6"

	var APIServer string
	var env string

	// pass parameters
	userpass := flag.String("user", "", "Defines optional username and password")
	token := flag.String("token", "", "Defines optional API token instead of username:password")
	fieldname := flag.String("field", "", "Defines optional field name , whose value should be returned instead of full information")
	testenvFlag := flag.Bool("testenv", false, "Use test environment (qa) instead or production")
	devenvFlag := flag.Bool("devenv", false, "Use development environment instead or production")
	showVersion := flag.Bool("version", false, "Show version number and exit")

	flag.Parse()

	if *showVersion {
		fmt.Printf("%s\n", VERSION)
		return
	}

	// check for program version only if running interactively
	datasetUtils.CheckForNewVersion(client, APP, VERSION, true)

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

	color.Set(color.FgGreen)
	log.Printf("You are about to retrieve the proposal information from the === %s === data catalog environment...", env)
	color.Unset()

	args := flag.Args()
	ownerGroup := ""

	//TODO cleanup text formatting:
	if len(args) == 1 {
		ownerGroup = args[0]
	} else {
		fmt.Printf("\n\nTool to retrieve proposal information for a given ownerGroup.\n\n")
		fmt.Printf("Run script with the following options and parameter:\n\n")
		fmt.Printf("datasetGetProposal [options]  ownerGroup\n\n")
		flag.PrintDefaults()
		fmt.Printf("\n\nFor further help see " + MANUAL + "\n\n")
		return
	}

	user, accessGroups := datasetUtils.Authenticate(client, APIServer, token, userpass)
	proposal := datasetUtils.GetProposal(client, APIServer, ownerGroup, user, accessGroups)
	// proposal is of type map[string]interface{}

	if len(proposal) > 0 {
		if *fieldname != "" {
			fmt.Println(proposal[*fieldname])
		} else {
			pretty, _ := json.MarshalIndent(proposal, "", "    ")
			fmt.Printf("%s\n", pretty)
		}
		os.Exit(0)
	} else {
		log.Printf("No Proposal information found for group %v\n", ownerGroup)
		os.Exit(1)
	}
}
