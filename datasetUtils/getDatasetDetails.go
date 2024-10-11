package datasetUtils

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strings"
)

type Dataset struct {
	Pid           string
	SourceFolder  string
	Size          int
	OwnerGroup    string
	NumberOfFiles int
}

/*
GetDatasetDetails retrieves details of datasets from a given API server. It filters the datasets by owner group if provided.

Parameters:
- client: The HTTP client used to send the request.
- APIServer: The URL of the API server.
- accessToken: The access token for authentication.
- datasetList: A list of dataset IDs to retrieve details for.
- ownerGroup: The owner group to filter the datasets by. If empty, no filtering is performed.

The function sends HTTP GET requests to the API server in chunks of 100 datasets at a time. It constructs a filter query parameter for the request using the dataset IDs and the owner group. If the response status code is 200, it reads the response body and unmarshals the JSON into a slice of Dataset structs. It then checks if details were found for all datasets in the chunk. If details were found for a dataset and the owner group matches the filter (or no filter is provided), it adds the dataset to the output slice. If no details were found for a dataset, it logs a message. If the response status code is not 200, it logs an error message.

Returns:
- A slice of Dataset structs containing the details of the datasets that match the owner group filter.
*/
func GetDatasetDetails(client *http.Client, APIServer string, accessToken string, datasetList []string, ownerGroup string) ([]Dataset, []string, error) {
	var returnedDatasets []Dataset
	var missingDatasetIds []string

	// split large request into chunks
	chunkSize := 100
	for i := 0; i < len(datasetList); i += chunkSize {
		end := i + chunkSize
		if end > len(datasetList) {
			end = len(datasetList)
		}

		filter := `{"where":{"pid":{"inq":["` +
			strings.Join(datasetList[i:end], `","`) +
			`"]}},"fields":{"pid":true,"sourceFolder":true,"size":true,"ownerGroup":true}}`

		v := url.Values{}
		v.Set("filter", filter)
		myurl := APIServer + "/Datasets?" + v.Encode()

		datasetDetails, err := fetchDatasetDetails(client, accessToken, myurl)
		if err != nil {
			return nil, nil, err
		}

		for _, datasetId := range datasetList[i:end] {
			datasetHasIdAndOwnerGroup := func(dataset Dataset) bool {
				return dataset.Pid == datasetId && (ownerGroup == "" || dataset.OwnerGroup == ownerGroup)
			}

			i := slices.IndexFunc(datasetDetails, datasetHasIdAndOwnerGroup) // linear search!
			if i >= 0 {
				returnedDatasets = append(returnedDatasets, datasetDetails[i]) // found id
			} else {
				missingDatasetIds = append(missingDatasetIds, datasetId) // id missing
			}
		}
	}
	return returnedDatasets, missingDatasetIds, nil
}

func fetchDatasetDetails(client *http.Client, token string, url string) ([]Dataset, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("querying dataset details failed with status code %v", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	datasetDetails := make([]Dataset, 0)
	err = json.Unmarshal(body, &datasetDetails)
	if err != nil {
		return nil, err
	}

	return datasetDetails, nil
}
