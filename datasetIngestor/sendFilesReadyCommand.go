package datasetIngestor

import (
	"bytes"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"strings"
)

/*
SendFilesReadyCommand is a function that sends a PUT request to a specified API server to update the dataset lifecycle status. 

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
func SendFilesReadyCommand(client *http.Client, APIServer string, datasetId string, user map[string]string) {
	var metaDataMap = map[string]interface{}{}
	metaDataMap["datasetlifecycle"] = map[string]interface{}{}
	metaDataMap["datasetlifecycle"].(map[string]interface{})["archiveStatusMessage"] = "datasetCreated"
	metaDataMap["datasetlifecycle"].(map[string]interface{})["archivable"] = true

	cmm, _ := json.Marshal(metaDataMap)
	// metadataString := string(cmm)

	myurl := APIServer + "/Datasets/" + strings.Replace(datasetId, "/", "%2F", 1) + "?access_token=" + user["accessToken"]
	req, err := http.NewRequest("PUT", myurl, bytes.NewBuffer(cmm))
	req.Header.Set("Content-Type", "application/json")
	//fmt.Printf("request to message broker:%v\n", req)
	resp, err := client.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == 200 {
		body, _ := io.ReadAll(resp.Body)
		log.Printf("Successfully updated %v\n", string(body))
	} else {
		log.Fatalf("SendFilesReadyCommand: Failed to update datasetLifecycle %v %v\n", resp.StatusCode, metaDataMap)
	}
}
