package datasetUtils

import (
	"fmt"
	"log"
	"os/exec"
	"strings"
)

func GetAvailableDatasets(username string, RSYNCServer string, singleDatasetId string) []string {
	datasetList := make([]string, 0)
	if singleDatasetId != "" {
		// Append missing prefix if needed
		if strings.HasPrefix(singleDatasetId, "20.500.11935") {
			datasetList = append(datasetList, singleDatasetId)
		} else {
			datasetList = append(datasetList, "20.500.11935/"+singleDatasetId)
		}
	} else {
		fmt.Printf("\n\n\n====== Checking for available datasets on archive cache server %s:\n", RSYNCServer)
		fmt.Printf("====== (only datasets highlighted in green will be retrieved)\n\n")
		fmt.Printf("====== If you can not find the dataset in this listing: may be you forgot\n")
		fmt.Printf("====== to start the necessary retrieve job from the the data catalog first ?\n\n")
		cmd := exec.Command("rsync", "-e", "ssh -q", "--list-only", username+"@"+RSYNCServer+":retrieve/")
		out, err := cmd.Output()
		if err != nil {
			log.Printf("Running %v.\n", cmd.Args)
			log.Fatal(err)
		}

		remoteListing := strings.Split(string(out), "\n")
		// fmt.Println("Remote Listing:",remoteListing)
		for _, fileLine := range remoteListing {
			columns := strings.Fields(fileLine)
			if len(columns) == 5 {
				if strings.HasPrefix(columns[0], "d") {
					if len(columns[4]) == 36 {
						datasetList = append(datasetList, "20.500.11935/"+columns[4])
					}
				}
			}
		}
	}
	return datasetList
}
