package datasetIngestor

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
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

func TestForExistingSourceFolder(folders []string, client *http.Client, APIServer string, accessToken string, allowExistingSourceFolder *bool) {
	// Split into chunks of 100 sourceFolders
	const chunkSize = 100
	all := len(folders)
	chunks := (all-1)/chunkSize + 1
	var sourceFolderList string
	header := `{"where":{"sourceFolder":{"inq":["`
	tail := `"]}},"fields": {"pid":1,"size":1,"sourceFolder":1}}`
	url := APIServer + "/Datasets?access_token=" + accessToken

	if allowExistingSourceFolder == nil || !(*allowExistingSourceFolder) {
		for i := 0; i < chunks; i++ {
			start := i * chunkSize
			end := start + chunkSize
			if end > all {
				end = all
			}
			log.Printf("Checking sourceFolder %v to %v for existing entries...\n", start+1, end)

			sourceFolderList = strings.Join(folders[start:end], "\",\"")

			// assemble filter
			filter := fmt.Sprintf("%s%s%s", header, sourceFolderList, tail)

			req, err := http.NewRequest("GET", url, nil)
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("filter", filter)

			resp, err := client.Do(req)
			if err != nil {
				log.Fatal(err)
			}
			defer resp.Body.Close()

			body, _ := ioutil.ReadAll(resp.Body)
			buf := new(strings.Builder)
			_, err = io.Copy(buf, bytes.NewReader(body))
			// check errors
			// log.Println(buf.String())
			var respObj QueryResult
			err = json.Unmarshal(body, &respObj)
			if err != nil {
				log.Fatal(err)
			}

			//fmt.Printf("response Object:\n%v\n", respObj)

			if len(respObj) > 0 {
				color.Set(color.FgYellow)
				log.Printf("Warning: The following sourceFolders have already been used")
				var item DatasetInfo
				for _, item = range respObj {
					log.Printf("Folder: %v, size: %v, PID: %v", item.SourceFolder, item.Size, item.Pid)
				}
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
					log.Fatalf("Use the flag -allowexistingsource to ingest nevertheless\n")
				}
				color.Unset()
			}
		}
	}
}
