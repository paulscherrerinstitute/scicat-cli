package datasetUtils

import (
	"fmt"
	"log"
	"os/exec"
	"strings"
	version "github.com/mcuadros/go-version"
	"regexp"
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

		// Get rsync version
		versionNumber, err := getRsyncVersion()
		if err != nil {
			log.Fatal("Error getting rsync version: ", err)
		}

		// Check rsync version and adjust command accordingly
		var cmd *exec.Cmd
		if version.Compare(versionNumber, "3.2.3", ">=") {
			cmd = exec.Command("rsync", "-e", "ssh", "--list-only", username+"@"+RSYNCServer+":retrieve/")
		} else {
			cmd = exec.Command("rsync", "-e", "ssh -q", "--list-only", username+"@"+RSYNCServer+":retrieve/")
		}

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

// Get rsync version
func getRsyncVersion() (string, error) {
	cmd := exec.Command("/usr/bin/rsync", "--version")
	output, err := cmd.Output()
	if err != nil {
		return "", err
	}
	version := string(output)
	
	// Use a regular expression to find the version number
	re := regexp.MustCompile(`\d+\.\d+\.\d+`)
	versionNumber := re.FindString(version)
	if versionNumber == "" {
		return "", fmt.Errorf("could not find version number in rsync version string: %s", version)
	}
	
	return versionNumber, nil
}
