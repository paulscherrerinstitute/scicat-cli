package datasetUtils

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"

	"github.com/fatih/color"
)

type DatasetInfo struct {
	Pid          string `json:"pid"`
	SourceFolder string `json:"sourceFolder"`
	Size         int    `json:"size"`
}
type QueryResult []DatasetInfo

// function that assembles the datasetIds to be fetched in chunks
// see https://blog.golang.org/slices for explanation why datasetList slice should be a return parameter

/*
addResult is a helper function that sends a GET request to the API server to fetch dataset details and appends the IDs of the datasets that are archivable to the datasetList.

Parameters:
- client: An instance of http.Client used to send the request.
- APIServer: The URL of the API server.
- filter: The filter query to be used in the GET request.
- accessToken: The access token used for authentication.
- datasetList: The list of dataset IDs to which the IDs of the archivable datasets will be appended.

The function first constructs the URL for the GET request by appending the filter and access token to the APIServer URL. It then sends the GET request and reads the response.

If the status code of the response is 200, the function reads the body of the response and unmarshals it into a QueryResult object. It then iterates over the datasets in the QueryResult. If a dataset's size is greater than 0, the function logs the dataset's details and appends its ID to the datasetList. If a dataset's size is 0, the function logs the dataset's details in red and does not append its ID to the datasetList.

If the status code of the response is not 200, the function logs the status code.

The function returns the updated datasetList.

Note: The function logs a fatal error and terminates the program if it fails to send the GET request or unmarshal the response body.
*/
func addResult(client *http.Client, APIServer string, filter string, accessToken string, datasetList []string) ([]string, error) {
	v := url.Values{}
	v.Set("filter", filter)

	myurl := fmt.Sprintf("%s/datasets?%s", APIServer, v.Encode())

	var body []byte
	req, err := http.NewRequest("GET", myurl, bytes.NewBuffer(body))
	if err != nil {
		return nil, fmt.Errorf("get dataset details request creation failed: %s", err.Error())
	}
	req.Header.Set("Authorization", "Bearer "+accessToken)

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("get dataset details request failed: %s", err.Error())
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	body, err = io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response body failed: %w", err)
	}

	var respObj QueryResult
	err = json.Unmarshal(body, &respObj)
	if err != nil {
		return nil, fmt.Errorf("unmarshal response body failed: %w", err)
	}

	if len(respObj) > 0 {
		log.Printf("Found the following datasets in state archivable: (size=0 datasets are removed)")
		for _, item := range respObj {
			if item.Size > 0 {
				log.Printf("Folder: %v, size: %v, PID: %v", item.SourceFolder, item.Size, item.Pid)
				datasetList = append(datasetList, item.Pid)
			} else {
				color.Set(color.FgRed)
				log.Printf("Folder: %v, size: %v, PID: %v will be ignored !", item.SourceFolder, item.Size, item.Pid)
				color.Unset()
			}
		}
	}

	return datasetList, nil
}

/*
GetArchivableDatasets retrieves a list of datasets that are eligible for archiving.

Parameters:
- client: An instance of http.Client used to send the request.
- APIServer: The URL of the API server.
- ownerGroup: The owner group of the datasets. If this is not empty, the function will fetch datasets belonging to this owner group. If it is empty, the function will fetch datasets based on the inputdatasetList.
- inputdatasetList: A list of dataset IDs. This is used only if ownerGroup is empty.
- accessToken: The access token used for authentication.

The function first checks if the ownerGroup is not empty. If it is not, it constructs a filter query to fetch datasets belonging to this owner group that are archivable. It then calls the addResult function to send the request and process the response.

If the ownerGroup is empty, the function splits the inputdatasetList into chunks and for each chunk, it constructs a filter query to fetch datasets with IDs in the chunk that are archivable. It then calls the addResult function for each chunk.

The function returns a list of dataset IDs that are archivable.

Note: A dataset is considered archivable if its size is greater than 0.
*/
func GetArchivableDatasets(client *http.Client, APIServer string, ownerGroup string, inputdatasetList []string, accessToken string) (datasetList []string, err error) {
	datasetList = make([]string, 0)

	filter := ""
	if len(inputdatasetList) == 0 {
		filter = `{"where":{"ownerGroup":"` + ownerGroup + `","datasetlifecycle.archivable":true},"fields": {"pid":1,"size":1,"sourceFolder":1}}`
		var err error
		datasetList, err = addResult(client, APIServer, filter, accessToken, datasetList)
		if err != nil {
			return datasetList, err
		}
	} else {
		// split large request into chunks
		chunkSize := 100
		for i := 0; i < len(inputdatasetList); i += chunkSize {
			end := i + chunkSize
			if end > len(inputdatasetList) {
				end = len(inputdatasetList)
			}
			quotedList := strings.Join(inputdatasetList[i:end], "\",\"")
			filter = `{"where":{"pid":{"inq":["` + quotedList + `"]},"datasetlifecycle.archivable":true},"fields": {"pid":1,"size":1,"sourceFolder":1}}`
			var err error
			datasetList, err = addResult(client, APIServer, filter, accessToken, datasetList)
			if err != nil {
				return datasetList, err
			}
		}
	}
	return datasetList, nil
}
