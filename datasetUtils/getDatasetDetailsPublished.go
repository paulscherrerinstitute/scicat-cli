package datasetUtils

import (
	"encoding/json"
	"github.com/fatih/color"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
)

const PUBLISHServer string = "doi2.psi.ch"

/*
GetDatasetDetailsPublished retrieves details of published datasets from a given API server. 

Parameters:
- client: An HTTP client used to send requests.
- APIServer: The URL of the API server from which to retrieve dataset details.
- datasetList: A list of dataset IDs for which to retrieve details.

The function sends HTTP GET requests to the API server, querying for details of datasets in chunks of 100 at a time. 
For each dataset, it checks if the details were found and if so, it logs the details, adds the dataset to the output list, 
and constructs a URL for the dataset. If the details were not found, it logs a message indicating that the dataset will not be copied.

The function returns two lists:
- A list of Dataset objects for which details were found.
- A list of URLs for the datasets.
*/
func GetDatasetDetailsPublished(client *http.Client, APIServer string, datasetList []string) ([]Dataset, []string) {
	log.Println("Dataset ID                                         Size[MB]  Owner                      SourceFolder")
	log.Println("====================================================================================================")

	// split large request into chunks
	chunkSize := 100
	for i := 0; i < len(datasetList); i += chunkSize {
		end := i + chunkSize
		if end > len(datasetList) {
			end = len(datasetList)
		}
		
		filter := createFilter(datasetList[i:end])
		
		resp, err := makeRequest(client, APIServer, filter)
		if err != nil {
			log.Fatal("Get dataset details failed:", err)
		}
		defer resp.Body.Close()
		
		if resp.StatusCode == 200 {
			outputDatasetDetails, urls := processDatasetDetails(resp, datasetList[i:end])
			return outputDatasetDetails, urls
		} else {
			log.Printf("Querying dataset details failed with status code %v\n", resp.StatusCode)
		}
	}
			
	return nil, nil
}

func createFilter(datasetList []string) string {
	return `{"where":{"pid":{"inq":["` +
	strings.Join(datasetList, `","`) +
	`"]}},"fields":{"pid":true,"sourceFolder":true,"size":true,"ownerGroup":true,"numberOfFiles":true}}`
}

func makeRequest(client *http.Client, APIServer string, filter string) (*http.Response, error) {
	v := url.Values{}
	v.Set("filter", filter)
	v.Add("isPublished", "true")
	
	var myurl = APIServer + "/Datasets?" + v.Encode()
	
	return client.Get(myurl)
}

func processDatasetDetails(resp *http.Response, datasetList []string) ([]Dataset, []string) {
	outputDatasetDetails := make([]Dataset, 0)
	urls := make([]string, 0)
	
	body, _ := io.ReadAll(resp.Body)
	
	datasetDetails := make([]Dataset, 0)
	
	_ = json.Unmarshal(body, &datasetDetails)
	
	for _, datasetId := range datasetList {
		detailsFound := false
		for _, datasetDetail := range datasetDetails {
			if datasetDetail.Pid == datasetId {
				detailsFound = true
				outputDatasetDetails = append(outputDatasetDetails, datasetDetail)
				color.Set(color.FgGreen)
				log.Printf("%s %9d %v %v\n", datasetId, datasetDetail.Size/1024./1024., datasetDetail.OwnerGroup, datasetDetail.SourceFolder)
				color.Unset()
				url := "https://" + PUBLISHServer + "/datasets" + datasetDetail.SourceFolder
				urls = append(urls, url)
				break
			}
		}
		if !detailsFound {
			color.Set(color.FgRed)
			log.Printf("Dataset %s no infos found in catalog - will not be copied !\n", datasetId)
			color.Unset()
		}
	}
	
	return outputDatasetDetails, urls
}
