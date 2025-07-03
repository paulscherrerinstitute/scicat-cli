package datasetIngestor

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
)

/*
CreateDatasetEntry is a function that creates a new dataset entry in a specified API server.

Parameters:
- client: An *http.Client object used to send the HTTP request.
- APIServer: A string representing the URL of the API server.
- metaDataMap: A map[string]interface{} containing the metadata for the new dataset.
- accessToken: A string representing the access token for the API server.

The function first serializes the metaDataMap to JSON, then sends a POST request to create the new dataset.

If the server responds with a status code of 200, the function decodes the response body to extract the "pid" field, which it returns as a string. If the server responds with any other status code, the function returns an error.

If the "type" field is not present in the metaDataMap, or if it contains an unrecognized value, the function logs an error message and terminates the program.

Returns:
- A string representing the "pid" of the newly created dataset. If the function encounters an error, it will not return a value.

Note: This function will terminate the program if it encounters an error, such as a failure to serialize the metaDataMap, a failure to send the HTTP request, a non-200 response from the server, or an unrecognized dataset type.
Note 2: This function is unused in cmd
*/
func CreateDatasetEntry(client *http.Client, APIServer string, metaDataMap map[string]interface{}, accessToken string) (datasetId string, err error) {
	// assemble json structure
	bm, err := json.Marshal(metaDataMap)
	if err != nil {
		return "", fmt.Errorf("couldn't marshal metadata map: %v", metaDataMap)
	}

	// send request
	req, err := http.NewRequest("POST", APIServer+"/Datasets"+"?access_token="+accessToken, bytes.NewBuffer(bm))
	if err != nil {
		return datasetId, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return "", fmt.Errorf("createDatasetEntry:Failed to create new dataset: status code %v", resp.StatusCode)
	}

	// decode response
	type PidType struct {
		Pid string `json:"pid"`
	}
	decoder := json.NewDecoder(resp.Body)
	var d PidType
	err = decoder.Decode(&d)
	if err != nil {
		return "", fmt.Errorf("could not decode pid from dataset entry: %v", err)
	}

	return d.Pid, nil
}
