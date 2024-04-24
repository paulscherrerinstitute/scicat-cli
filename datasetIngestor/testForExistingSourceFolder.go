package datasetIngestor

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"

	"github.com/fatih/color"
)

/* Check if sourceFolders have already been used by existing datasets and give warning

The idea is to send ONE query which tests all datasets in one go, (but see chunking need below)

The filter condition can be defined within the header instead of the url

The filter size limit is server dependent: typically 8kB for the header, for URL length 2 kB (IE)
Both limits may well be exceeed e.g. for 400 datasets
Therefore split query into many chunks if too many folders are used in one job

*/
type DatasetInfo struct {
	Pid          string `json:"pid"`
	SourceFolder string `json:"sourceFolder"`
	Size         int    `json:"size"`
}

type QueryResult []DatasetInfo

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
func TestForExistingSourceFolder(folders []string, client *http.Client, APIServer string, accessToken string, allowExistingSourceFolder *bool) {
	// Split into chunks of 100 sourceFolders
	const chunkSize = 100
	all := len(folders)
	chunks := (all-1)/chunkSize + 1
	url := APIServer + "/Datasets?access_token=" + accessToken
	
	if allowExistingSourceFolder == nil || !(*allowExistingSourceFolder) {
		for i := 0; i < chunks; i++ {
			start := i * chunkSize
			end := start + chunkSize
			if end > all {
				end = all
			}
			log.Printf("Checking sourceFolder %v to %v for existing entries...\n", start+1, end)
			
			sourceFolderList := strings.Join(folders[start:end], "\",\"")
			filter := createFilter(sourceFolderList)
			resp := makeRequest(client, url, filter)
			respObj := processResponse(resp)
			
			if len(respObj) > 0 {
				var item DatasetInfo
				for _, item = range respObj {
					log.Printf("Folder: %v, size: %v, PID: %v", item.SourceFolder, item.Size, item.Pid)
				}
				if !confirmIngestion(allowExistingSourceFolder) {
					log.Fatalf("Use the flag -allowexistingsource to ingest nevertheless\n")
				}
			}
		}
	}
}

func createFilter(sourceFolderList string) string {
	header := `{"where":{"sourceFolder":{"inq":["`
	tail := `"]}},"fields": {"pid":1,"size":1,"sourceFolder":1}}`
	return fmt.Sprintf("%s%s%s", header, sourceFolderList, tail)
}

func makeRequest(client *http.Client, url string, filter string) *http.Response {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		log.Fatal("Error creating request: ", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("filter", filter)
	
	resp, err := client.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()
	return resp
}

func processResponse(resp *http.Response) QueryResult {
	body, _ := io.ReadAll(resp.Body)
	var respObj QueryResult
	if len(body) == 0 {
		log.Printf("Warning: Response body is empty")
		return respObj
	}
	err := json.Unmarshal(body, &respObj)
	if err != nil {
		log.Printf("Error: Failed to parse JSON response: %v", err)
	}
	return respObj
}

func confirmIngestion(allowExistingSourceFolder *bool) bool {
	color.Set(color.FgYellow)
	log.Printf("Warning: The following sourceFolders have already been used")
	continueFlag := true
	if allowExistingSourceFolder == nil {
		log.Printf("Do you want to ingest the corresponding new datasets nevertheless (y/N) ? ")
		scanner.Scan()
		archiveAgain := scanner.Text()
		if archiveAgain != "y" {
			continueFlag = false
		}
	} else {
		continueFlag = *allowExistingSourceFolder
	}
	if continueFlag {
		log.Printf("You chose to continue the new datasets nevertheless\n")
	} else {
		log.Printf("You chose not to continue\n")
		log.Printf("Therefore the ingest process is stopped here, no datasets will be ingested\n")
	}
	color.Unset()
	return continueFlag
}
