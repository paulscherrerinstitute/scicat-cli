package datasetIngestor

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
)

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
		body, _ := ioutil.ReadAll(resp.Body)
		log.Printf("Successfully updated %v\n", string(body))
	} else {
		log.Fatalf("SendFilesReadyCommand: Failed to update datasetLifecycle %v %v\n", resp.StatusCode, metaDataMap)
	}

}
