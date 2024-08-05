package datasetIngestor

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

/*
MarkFilesReady is a function that sends a PUT request to a specified API server to update the dataset lifecycle status.

Parameters:
- client: An *http.Client object, used to send the HTTP request.
- APIServer: A string representing the URL of the API server.
- datasetId: A string representing the ID of the dataset to be updated.
- user: A map[string]string containing user information, specifically the access token.

The function constructs a metadata map with the dataset lifecycle status set to "datasetCreated" and archivable set to true.
This metadata is then converted to JSON and sent in the body of the PUT request.
The URL for the request is constructed using the APIServer and datasetId parameters, and the user's access token is appended as a query parameter.

If the request is successful (HTTP status code 200), the function logs a success message along with the response body.
If the request fails, the function logs a failure message along with the status code and metadata map.
*/
func MarkFilesReady(client *http.Client, APIServer string, datasetId string, user map[string]string) error {
	var metaDataMap = map[string]interface{}{}
	metaDataMap["datasetlifecycle"] = map[string]interface{}{}
	metaDataMap["datasetlifecycle"].(map[string]interface{})["archiveStatusMessage"] = "datasetCreated"
	metaDataMap["datasetlifecycle"].(map[string]interface{})["archivable"] = true

	cmm, _ := json.Marshal(metaDataMap)
	// metadataString := string(cmm)

	myurl := APIServer + "/Datasets/" + strings.Replace(datasetId, "/", "%2F", 1) + "?access_token=" + user["accessToken"]
	req, err := http.NewRequest("PUT", myurl, bytes.NewBuffer(cmm))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("failed to update datasetLifecycle %v %v", resp.StatusCode, metaDataMap)
	}
	return nil
}
