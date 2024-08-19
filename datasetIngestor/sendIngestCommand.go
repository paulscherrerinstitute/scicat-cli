package datasetIngestor

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
)

type FileBlock struct {
	Size         int64      `json:"size"`
	DataFileList []Datafile `json:"dataFileList"`
	DatasetId    string     `json:"datasetId"`
}

const TOTAL_MAXFILES = 400000
const BLOCK_MAXBYTES = 200000000000 // 700000 for testing the logic
const BLOCK_MAXFILES = 20000        // 20 for testing the logic

/*
	createOrigBlock generates a `FileBlock` from a subset of a given `filesArray`.

It takes start and end indices to determine the subset, and a datasetId to associate with the FileBlock.
The function calculates the total size of all Datafiles in the subset and includes this in the FileBlock.

Parameters:
start: The starting index of the subset in the filesArray.
end: The ending index of the subset in the filesArray.
filesArray: The array of Datafiles to create the FileBlock from.
datasetId: The id to associate with the FileBlock.

Returns:
A FileBlock that includes the total size of the Datafiles in the subset, the subset of Datafiles, and the datasetId.
*/
func createOrigBlock(start int, end int, filesArray []Datafile, datasetId string) (fileblock FileBlock) {
	// accumulate sizes
	var totalSize int64
	totalSize = 0
	for i := start; i < end; i++ {
		totalSize += filesArray[i].Size
	}

	return FileBlock{Size: totalSize, DataFileList: filesArray[start:end], DatasetId: datasetId}
}

/*
IngestDataset sends an ingest command to the API server to create a new dataset and associated data blocks.

Parameters:
client: The HTTP client used to send the request.
APIServer: The URL of the API server.
metaDataMap: A map containing metadata for the dataset.
fullFileArray: An array of Datafile objects representing the files in the dataset.
user: A map containing user information, including the access token.

The function first creates a new dataset by sending a POST request to the appropriate endpoint on the API server,
based on the dataset type specified in metaDataMap. The dataset type can be "raw", "derived", or "base".
If the dataset type is not one of these, the function logs a fatal error.

The function then creates original data blocks for the dataset. It splits the dataset into blocks if the dataset
contains more than a certain number of files or if the total size of the files exceeds a certain limit.
Each block is created by calling the createOrigBlock function and then sending a POST request to the "/OrigDatablocks"
endpoint on the API server.

If the total number of files in the dataset exceeds the maximum limit, the function logs a fatal error.

Returns:
The ID of the created dataset.
*/
func IngestDataset(client *http.Client, APIServer string, metaDataMap map[string]interface{},
	fullFileArray []Datafile, user map[string]string) (datasetId string, err error) {

	datasetId, err = createDataset(client, APIServer, metaDataMap, user)
	if err != nil {
		return datasetId, err
	}
	err = createOrigDatablocks(client, APIServer, fullFileArray, datasetId, user)

	return datasetId, err
}

func createDataset(client *http.Client, APIServer string, metaDataMap map[string]interface{}, user map[string]string) (string, error) {
	cmm, _ := json.Marshal(metaDataMap)
	datasetId := ""

	if val, ok := metaDataMap["type"]; ok {
		dstype := val.(string)
		endpoint, err := getEndpoint(dstype)
		if err != nil {
			return "", err
		}
		myurl := APIServer + endpoint + "/?access_token=" + user["accessToken"]
		resp, err := sendRequest(client, "POST", myurl, cmm)
		if err != nil {
			return "", err
		}
		defer resp.Body.Close()

		if resp.StatusCode == 200 {
			datasetId, err = decodePid(resp)
			if err != nil {
				return "", err
			}
		} else {
			return "", fmt.Errorf("SendIngestCommand: Failed to create new dataset: status code %v", resp.StatusCode)
		}
	} else {
		return "", fmt.Errorf("no dataset type defined for dataset %v", metaDataMap)
	}

	return datasetId, nil
}

func getEndpoint(dstype string) (string, error) {
	switch dstype {
	case "raw":
		return "/RawDatasets", nil
	case "derived":
		return "/DerivedDatasets", nil
	case "base":
		return "/Datasets", nil
	default:
		return "", fmt.Errorf("unknown dataset type encountered: %s", dstype)
	}
}

func sendRequest(client *http.Client, method, url string, body []byte) (*http.Response, error) {
	req, err := http.NewRequest(method, url, bytes.NewBuffer(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func decodePid(resp *http.Response) (string, error) {
	type PidType struct {
		Pid string `json:"pid"`
	}
	decoder := json.NewDecoder(resp.Body)
	var d PidType
	err := decoder.Decode(&d)
	if err != nil {
		return "", fmt.Errorf("could not decode pid from dataset entry: %v", err)
	}

	return d.Pid, nil
}

/*
	createOrigDatablocks sends a series of POST requests to the server to create original data blocks.

It divides the fullFileArray into blocks based on the BLOCK_MAXFILES and BLOCK_MAXBYTES constants, and sends a request for each block.

Parameters:

client: The HTTP client used to send the requests.
APIServer: The base URL of the API server.
fullFileArray: An array of Datafile objects representing the files in the dataset.
datasetId: The ID of the dataset.
user: A map containing user information. The "accessToken" key should contain the user's access token.

If the total number of files exceeds TOTAL_MAXFILES, the function logs a fatal error.
If a request receives a response with a status code other than 200, the function logs a fatal error.

The function logs a message for each created data block, including the start and end file, the total size, and the number of files in the block.
*/
func createOrigDatablocks(client *http.Client, APIServer string, fullFileArray []Datafile, datasetId string, user map[string]string) error {
	totalFiles := len(fullFileArray)

	if totalFiles > TOTAL_MAXFILES {
		return fmt.Errorf(
			"dataset exceeds (%v) the maximum number of files per dataset , which can currently be handled by the archiving system (%v)",
			totalFiles, TOTAL_MAXFILES)
	}

	end := 0
	var blockBytes int64
	for start := 0; end < totalFiles; {
		blockBytes = 0

		for end = start; end-start < BLOCK_MAXFILES && blockBytes < BLOCK_MAXBYTES && end < totalFiles; {
			blockBytes += fullFileArray[end].Size
			end++
		}
		origBlock := createOrigBlock(start, end, fullFileArray, datasetId)

		payloadString, _ := json.Marshal(origBlock)
		myurl := APIServer + "/OrigDatablocks" + "?access_token=" + user["accessToken"]
		resp, err := sendRequest(client, "POST", myurl, payloadString)
		if err != nil {
			return err
		}
		defer resp.Body.Close()

		if resp.StatusCode != 200 {
			return fmt.Errorf("unexpected response code \"%v\" when adding origDatablock for dataset id: \"%v\"", resp.Status, datasetId)
		}

		start = end
	}
	return nil
}
