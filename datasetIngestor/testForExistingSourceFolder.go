package datasetIngestor

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

/*
	Check if sourceFolders have already been used by existing datasets and give warning

The idea is to send ONE query which tests all datasets in one go, (but see chunking need below)

# The filter condition can be defined within the header instead of the url

The filter size limit is server dependent: typically 8kB for the header, for URL length 2 kB (IE)
Both limits may well be exceeed e.g. for 400 datasets
Therefore split query into many chunks if too many folders are used in one job
*/
type DatasetInfo struct {
	Pid          string `json:"pid"`
	SourceFolder string `json:"sourceFolder"`
	Size         int    `json:"size"`
	OwnerGroup   string `json:"ownerGroup"`
}

type DatasetQuery []DatasetInfo

/*
TestForExistingSourceFolder checks if the provided source folders already exist on the API server.

Parameters:
- folders: A slice of strings representing the source folders to check.
- client: An http.Client object used to send the HTTP requests.
- APIServer: A string representing the URL of the API server.
- accessToken: A string representing the access token for the API server.
- allowExistingSourceFolder: A pointer to a boolean. If it's nil or false, the function will check for existing source folders. If it's true, the function will not perform the check.

The function splits the folders into chunks of 100 and sends a GET request to the API server for each chunk. If a source folder already exists, the function logs a warning and asks the user if they want to continue. If the user chooses not to continue, the function stops the process and logs an error message.
*/
func TestForExistingSourceFolder(folders []string, client *http.Client, APIServer string, accessToken string) (foundList DatasetQuery, err error) {
	// Split into chunks of 100 sourceFolders
	const chunkSize = 100
	all := len(folders)
	chunks := (all-1)/chunkSize + 1

	for i := 0; i < chunks; i++ {
		start := i * chunkSize
		end := start + chunkSize
		if end > all {
			end = all
		}

		filter := createFilter(folders[start:end])
		resp, err := datasetSearchRequest(client, APIServer, accessToken, filter)
		if err != nil {
			return DatasetQuery{}, err
		}
		defer resp.Body.Close()
		processedResp, err := processResponse(resp)
		if err != nil {
			return foundList, err
		}
		foundList = append(foundList, processedResp...)
	}
	return foundList, err
}

func createFilter(sourceFolderList []string) string {
	header := `{"where":{"sourceFolder":{"inq":["`
	tail := `"]}},"fields": {"pid":1,"size":1,"sourceFolder":1,"ownerGroup":1}}`
	return fmt.Sprintf("%s%s%s", header, strings.Join(sourceFolderList, "\",\""), tail)
}

func datasetSearchRequest(client *http.Client, APIServer string, token string, filter string) (*http.Response, error) {
	req, err := http.NewRequest("GET", APIServer+"/datasets?filter="+url.QueryEscape(filter), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func processResponse(resp *http.Response) (DatasetQuery, error) {
	body, _ := io.ReadAll(resp.Body)
	var respObj DatasetQuery
	if len(body) == 0 {
		// ignoring empty response...
		return respObj, nil
	}
	err := json.Unmarshal(body, &respObj)
	if err != nil {
		return DatasetQuery{}, fmt.Errorf("failed to parse JSON response: %v", err)
	}
	return respObj, nil
}
