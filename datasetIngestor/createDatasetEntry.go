package datasetIngestor

import (
	"bytes"
	"encoding/json"
	"errors"
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

The function first serializes the metaDataMap to JSON. It then checks the "type" field in the metaDataMap to determine the type of the dataset ("raw", "derived", or "base"). Depending on the type, it constructs the appropriate URL for the API server and sends a POST request to create the new dataset.

If the server responds with a status code of 200, the function decodes the response body to extract the "pid" field, which it returns as a string. If the server responds with any other status code, the function logs an error message and terminates the program.

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
	if val, ok := metaDataMap["type"]; ok {
		dstype := val.(string)
		// fmt.Println(errm,sourceFolder)

		// verify data structure of meta data by calling isValid API for Dataset

		myurl := ""
		if dstype == "raw" {
			myurl = APIServer + "/RawDatasets"
		} else if dstype == "derived" {
			myurl = APIServer + "/DerivedDatasets"
		} else if dstype == "base" {
			myurl = APIServer + "/Datasets"
		} else {
			return "", fmt.Errorf("unknown dataset type encountered: %v", dstype)
		}

		req, err := http.NewRequest("POST", myurl+"?access_token="+accessToken, bytes.NewBuffer(bm))
		if err != nil {
			return datasetId, err
		}
		req.Header.Set("Content-Type", "application/json")
		//fmt.Printf("request to message broker:%v\n", req)
		resp, err := client.Do(req)
		if err != nil {
			return "", err
		}
		defer resp.Body.Close()
		if resp.StatusCode == 200 {
			// important: use capital first character in field names!
			type PidType struct {
				Pid string `json:"pid"`
			}
			decoder := json.NewDecoder(resp.Body)
			var d PidType
			err := decoder.Decode(&d)
			if err != nil {
				return "", fmt.Errorf("could not decode pid from dataset entry: %v", err)
			}
			// fmtlog.Printf("Extracted pid:%s", d.Pid)
			return d.Pid, nil
		} else {
			return "", fmt.Errorf("createDatasetEntry:Failed to create new dataset: status code %v", resp.StatusCode)
		}
	} else {
		return "", errors.New("type of dataset not defined")
	}
}
