/*

Purpose: Create Job to retrieve all datasets of a given PublishedData item

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
	"time"

	"github.com/fatih/color"
)

const PROD_API_SERVER string = "https://dacat.psi.ch/api/v3"
const TEST_API_SERVER string = "https://dacat-qa.psi.ch/api/v3"
const DEV_API_SERVER string = "https://dacat-development.psi.ch/api/v3"

const PROD_RSYNC_RETRIEVE_SERVER string = "pb-retrieve.psi.ch"
const TEST_RSYNC_RETRIEVE_SERVER string = "pbt-retrieve.psi.ch"
const DEV_RSYNC_RETRIEVE_SERVER string = "arematest2in.psi.ch"

const PUBLISHServer string = "doi2.psi.ch"
const RETRIEVELocation string = "/data/archiveManager/retrieve/"

// const PROD_RSYNC_RETRIEVE_SERVER string = "ebarema4in.psi.ch"
// const TEST_RSYNC_RETRIEVE_SERVER string = "ebaremat1in.psi.ch"
// const DEV_RSYNC_RETRIEVE_SERVER string = "arematest2in.psi.ch"

const MANUAL string = "http://melanie.gitpages.psi.ch/SciCatPages/#sec-5"

const APP = "datasetPublishDataRetrieve"

var APIServer string = PROD_API_SERVER
var RSYNCServer string = PROD_RSYNC_RETRIEVE_SERVER

var client = &http.Client{
	Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: false}},
	Timeout:   10 * time.Second}
var scanner = bufio.NewScanner(os.Stdin)
var VERSION string

func main() {
	// check input parameters
	retrieveFlag := flag.Bool("retrieve", false, "Defines if this command is meant to actually retrieve data (default: retrieve actions are only displayed)")
	publishedDataId := flag.String("publisheddata", "", "Defines to publish data from a given publishedData document ID")
	userpass := flag.String("user", "", "Defines optional username:password string")
	token := flag.String("token", "", "Defines optional API token instead of username:password")
	testenvFlag := flag.Bool("testenv", false, "Use test environment (qa) (default is to use production system)")
	devenvFlag := flag.Bool("devenv", false, "Use development environment (default is to use production system)")
	showVersion := flag.Bool("version", false, "Show version number and exit")

	flag.Parse()

	if *showVersion {
		fmt.Printf("%s\n", VERSION)
		return
	}

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
	log.Printf("You are about to trigger a retrieve job for publish dataset(s) from the === %s === retrieve server...", env)
	color.Unset()

	if *retrieveFlag == false {
		color.Set(color.FgRed)
		log.Printf("Note: you run in 'dry' mode to simply check which data would be retrieved.\n")
		log.Printf("Use the -retrieve flag to actually retrieve the datasets.\n")
		color.Unset()
	}

	if *publishedDataId == "" { /* && *datasetId == "" && *ownerGroup == "" */
		fmt.Println("\n\nTool to retrieve datasets to the intermediate cache server of the tape archive")
		fmt.Printf("Run script without arguments, but specify options:\n\n")
		fmt.Printf("datasetPublishDataRetrieve [options] \n\n")
		fmt.Printf("Use -publisheddata option to define the datasets which should be published.\n\n")
		fmt.Printf("For example:\n")
		fmt.Printf("./datasetPublishDataRetrieve -user archiveManager:password -publisheddata 10.16907/05a50450-767f-421d-9832-342b57c201\n\n")
		fmt.Printf("The script should be run as archiveManager\n\n")
		flag.PrintDefaults()
		return
	}

	user, _ := datasetUtils.Authenticate(client, APIServer, token, userpass)

	datasetList, _, _ := datasetUtils.GetDatasetsOfPublication(client, APIServer, *publishedDataId)

	// get sourceFolder and other dataset related info for all Datasets and print them
	datasetUtils.GetDatasetDetailsPublished(client, APIServer, datasetList)

	if !*retrieveFlag {
		color.Set(color.FgRed)
		log.Printf("\n\nNote: you run in 'dry' mode to simply check what would happen.")
		log.Printf("Use the -retrieve flag to actually retrieve data from tape.\n")
		color.Unset()
	} else {
		// create retrieve Job
		jobId, err := datasetUtils.CreateRetrieveJob(client, APIServer, user, datasetList)
		if err != nil {
			log.Fatal(err)
		} else{
			fmt.Println(jobId)
		}
	}
}
