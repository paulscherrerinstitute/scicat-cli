package datasetUtils

import (
	"fmt"
	"os/exec"
	"strings"
	version "github.com/mcuadros/go-version"
	"regexp"
)

/*
GetAvailableDatasets retrieves a list of available datasets from a remote RSYNC server.

Parameters:
- username: The username to use when connecting to the RSYNC server.
- RSYNCServer: The address of the RSYNC server to connect to.
- singleDatasetId: An optional parameter. If provided, the function will return a list containing only this dataset ID. If the ID does not start with "20.500.11935", this prefix will be added. If this parameter is an empty string, the function will retrieve a list of all available datasets from the RSYNC server.

The function first checks if a singleDatasetId is provided. If so, it adds it to the dataset list, with the necessary prefix if needed. If not, it connects to the RSYNC server and retrieves a list of all available datasets. The function checks the version of rsync and adjusts the command accordingly. It then parses the output, adding each dataset ID to the list.

Returns:
- A slice of strings, where each string is a dataset ID.
*/
func GetAvailableDatasets(username string, RSYNCServer string, singleDatasetId string) ([]string, error) {
	datasetList := make([]string, 0)
	if singleDatasetId != "" {
		datasetList = append(datasetList, formatDatasetId(singleDatasetId))
	} else {
		printMessage(RSYNCServer)
		datasets, err := fetchDatasetsFromServer(username, RSYNCServer)
		if err != nil {
			return nil, err
		}
		datasetList = append(datasetList, datasets...)
	}
	return datasetList, nil
}

func formatDatasetId(datasetId string) string {
	if strings.HasPrefix(datasetId, "20.500.11935") {
		return datasetId
	}
	return "20.500.11935/" + datasetId
}

func fetchDatasetsFromServer(username string, RSYNCServer string) ([]string, error) {
	versionNumber, err := getRsyncVersion()
	if err != nil {
		return nil, fmt.Errorf("error getting rsync version: %w", err)
	}
	
	cmd := buildRsyncCommand(username, RSYNCServer, versionNumber)
	out, err := cmd.Output()
	if err != nil {
		return nil, err
	}
	
	return parseRsyncOutput(out), nil
}

func buildRsyncCommand(username string, RSYNCServer string, versionNumber string) *exec.Cmd {
	if version.Compare(versionNumber, "3.2.3", ">=") {
		return exec.Command("rsync", "-e", "ssh", "--list-only", username+"@"+RSYNCServer+":retrieve/")
	}
	return exec.Command("rsync", "-e", "ssh -q", "--list-only", username+"@"+RSYNCServer+":retrieve/")
}

func parseRsyncOutput(output []byte) []string {
	remoteListing := strings.Split(string(output), "\n")
	datasets := make([]string, 0)
	for _, fileLine := range remoteListing {
		columns := strings.Fields(fileLine)
		if len(columns) == 5 && strings.HasPrefix(columns[0], "d") && len(columns[4]) == 36 {
			datasets = append(datasets, "20.500.11935/"+columns[4])
		}
	}
	return datasets
}

// Get rsync version
var getRsyncVersion = func() (string, error) {
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

func printMessage(RSYNCServer string) {
	var message strings.Builder
	message.WriteString("\n\n\n====== Checking for available datasets on archive cache server ")
	message.WriteString(RSYNCServer)
	message.WriteString(":\n====== (only datasets highlighted in green will be retrieved)\n\n")
	message.WriteString("====== If you can not find the dataset in this listing: may be you forgot\n")
	message.WriteString("====== to start the necessary retrieve job from the the data catalog first?\n\n")
	fmt.Print(message.String())
}
