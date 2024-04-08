/*

Purpose: copy all files from a publisheddata entry (list of datasets) to publication server
         taking into account original sourceFolder names

This script must be run on the retrieve servers (from root) and pushes data to the publication server
hosted in the DMZ. It requires that a previous retrieve job for the datasets, executed
by the user "archiveManager", is finished, such that data are available in the retrieve
location

The resulting files from dataset folders will be stored under the full original sourcePath
on the publication server

*/

package main

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"html/template"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/exec"
	"github.com/paulscherrerinstitute/scicat/datasetUtils"
	"strings"
	"time"
	"unicode/utf8"

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

const APP = "datasetPublishData"

var APIServer string = PROD_API_SERVER
var RSYNCServer string = PROD_RSYNC_RETRIEVE_SERVER

var client = &http.Client{
	Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: false}},
	Timeout:   10 * time.Second}
var scanner = bufio.NewScanner(os.Stdin)
var VERSION string

type PageData struct {
	Doi           string
	PageTitle     string
	BrowseUrls    []string
	SizeArray     []int
	NumFilesArray []int
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func main() {
	// check input parameters
	publishFlag := flag.Bool("publish", false, "Defines if this command is meant to actually publish data (default nothing is done)")
	publishedDataId := flag.String("publisheddata", "", "Defines to publish data froma given publishedData document ID")
	// datasetId := flag.String("dataset", "", "Defines single datasetId to publish")
	// ownerGroup := flag.String("ownergroup", "", "Defines to publish only datasets of the specified ownerGroup")
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
	log.Printf("You are about to publish dataset(s) from the === %s === retrieve server...", env)
	color.Unset()

	if *publishFlag == false {
		color.Set(color.FgRed)
		log.Printf("Note: you run in 'dry' mode to simply check which data would be published.\n")
		log.Printf("Use the -publish flag to actually publish the datasets.\n")
		color.Unset() // Don't forget to unset
	}

	if *publishedDataId == "" { /* && *datasetId == "" && *ownerGroup == "" */
		fmt.Println("\n\nTool to publish datasets from the intermediate cache server of the tape archive")
		fmt.Printf("to the publication server. Copies the files, creates and installs a download page\n")
		fmt.Printf("and updates the downloadLink value for the specified PublishedData document\n\n")
		fmt.Printf("Run script without arguments, but specify options:\n\n")
		fmt.Printf("datasetPublishData [options] \n\n")
		fmt.Printf("Use -publisheddata option to define the datasets which should be published.\n\n")
		fmt.Printf("For example:\n")
		fmt.Printf("./datasetPublishData -user archiveManager:password -publisheddata 10.16907/05a50450-767f-421d-9832-342b57c201\n\n")
		fmt.Printf("To update the PublishedData entry with the downloadLink you have to run the script as user archiveManager\n\n")
		flag.PrintDefaults()
		return
	}

	datasetList, title, doi := datasetUtils.GetDatasetsOfPublication(client, APIServer, *publishedDataId)

	// get sourceFolder and other dataset related info for all Datasets
	datasetDetails, urls := datasetUtils.GetDatasetDetailsPublished(client, APIServer, datasetList)

	// assemble rsync commands to be submitted
	batchCommands := assembleRsyncCommands(datasetDetails)

	if !*publishFlag {
		color.Set(color.FgRed)
		log.Printf("\n\nNote: you run in 'dry' mode to simply check what would happen.")
		log.Printf("Use the -publish flag to actually copy data to publication server.")
		log.Printf("The following commands will be executed")
		log.Printf("%v\n", strings.Join(batchCommands[:], "\n\n"))
		color.Unset()
	} else {
		executeCommands(batchCommands)
		createWebpage(urls, title, doi, datasetDetails, *publishedDataId, userpass, token)
	}
}

func assembleRsyncCommands(datasetDetails []datasetUtils.Dataset) []string {
	batchCommands := make([]string, 0)
	for _, dataset := range datasetDetails {
		shortDatasetId := strings.Split(dataset.Pid, "/")[1]
		fullDest := "/datasets" + dataset.SourceFolder
		command := "ssh " + PUBLISHServer + " mkdir -p " + fullDest + ";" +
			"ssh " + PUBLISHServer + " chown -R egli " + fullDest + ";" +
			"ssh " + PUBLISHServer + " chmod -R 755 " + fullDest + ";" +
			"/usr/bin/rsync -av -e ssh " + RETRIEVELocation + shortDatasetId + "/ " + PUBLISHServer + ":" + fullDest
		batchCommands = append(batchCommands, command)
	}
	return batchCommands
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

// return shortest string, length given in bytes
func findMinLength(arr []string) int {
	n := len(arr)
	min := len(arr[0])

	for i := 1; i < n; i++ {
		if len(arr[i]) < min {
			min = len(arr[i])
		}
	}

	return min
}

// A Function that returns the longest common prefix path (runes)
// from the array of strings
func commonPrefix(arr []string) string {
	n := len(arr)
	if n == 1 {
		return arr[0]
	}

	minlenBytes := findMinLength(arr)

	result := "" // Our resultant string

	// loop over runes (UTF8)

	for i, w := 0, 0; i < minlenBytes; i += w {
		currentRune, width := utf8.DecodeRuneInString(arr[0][i:])
		// fmt.Printf("%#U starts at byte position %d\n", currentRune, i)
		w = width
		// loop through other strings
		for j := 1; j < n; j++ {
			nextRune, _ := utf8.DecodeRuneInString(arr[j][i:])
			if nextRune != currentRune {
				// strip off characters after last "/"
				parts := strings.Split(result, "/")
				result = strings.Join(parts[:len(parts)-1], "/") + "/"
				return result
			}
		}
		result = result + string(currentRune)
	}
	// strip off characters after last "/"
	parts := strings.Split(result, "/")
	result = strings.Join(parts[:len(parts)-1], "/") + "/"
	return result
}

func stringInSlice(a string, list []string) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}

func createWebpage(urls []string, title string, doi string, datasetDetails []datasetUtils.Dataset,
	publishedDataId string, userpass *string, token *string) {
	// log.Printf("Datasetdetails %v", datasetDetails)
	tmpl := template.Must(template.ParseFiles("downloadPage.html"))
	sizeArray := make([]int, 0)
	numFilesArray := make([]int, 0)
	for _, datasetDetail := range datasetDetails {
		sizeArray = append(sizeArray, datasetDetail.Size)
		numFilesArray = append(numFilesArray, datasetDetail.NumberOfFiles)
	}
	data := PageData{
		Doi:           doi,
		PageTitle:     title,
		BrowseUrls:    urls,
		SizeArray:     sizeArray,
		NumFilesArray: numFilesArray,
	}

	// log.Printf("Pagedata %v", data)
	f, err := os.Create("output.html")
	check(err)
	defer f.Close()
	tmpl.Execute(f, data)

	// determine location of downloadLink from common part of all sourceFolders
	downloadLink := commonPrefix(urls)
	fmt.Printf("downloadLink:%v\n", downloadLink)
	// move up one level in case that one dataset sourcefolder is equal to downloadLocation
	// to avoidto "hide" the sourcefolder loaction by the index.html
	if stringInSlice(downloadLink, urls) {
		slice := strings.Split(downloadLink, "/")
		if len(slice) > 0 {
			slice = slice[:len(slice)-1]
		}
		downloadLink = strings.Join(slice, "/")
	}
	fmt.Printf("downloadLink2 :%v\n", downloadLink)

	// copy output.html to downloadLink location (remove https://server part) as index.html
	startPos := strings.Index(downloadLink, "/datasets")
	command := "/usr/bin/rsync -av -e ssh output.html " + PUBLISHServer + ":" + downloadLink[startPos:] + "/index.html"
	cmd := exec.Command("/bin/sh", "-c", command)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	log.Printf("\n=== Transfer download page command: %s .\n", command)
	err2 := cmd.Run()
	if err != nil {
		log.Fatal(err2)
	}

	// set value in publishedData ==============================

	user, _ := datasetUtils.Authenticate(client, APIServer, token, userpass)

	type PublishedDataPart struct {
		DownloadLink string `json:"downloadLink"`
	}
	updateData := PublishedDataPart{
		DownloadLink: downloadLink,
	}

	cmm, _ := json.Marshal(updateData)
	// metadataString := string(cmm)

	myurl := APIServer + "/PublishedData/" + strings.Replace(publishedDataId, "/", "%2F", 1) + "?access_token=" + user["accessToken"]
	req, err := http.NewRequest("PATCH", myurl, bytes.NewBuffer(cmm))
	if err != nil {
		log.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	// fmt.Printf("request to message broker:%v\n", req)
	resp, err := client.Do(req)
	if err != nil {
		log.Fatal(err)
	}

	defer resp.Body.Close()
	if resp.StatusCode == 200 {
		ioutil.ReadAll(resp.Body)
		log.Printf("Successfully set downloadLink to %v\n", downloadLink)
	} else {
		log.Fatalf("Failed to update downloadLink on publishedData %v %v\n", resp.StatusCode, publishedDataId)
	}
}
