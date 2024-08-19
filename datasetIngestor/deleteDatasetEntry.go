package datasetIngestor

import (
	"net/http"
	"strings"
)

// note: this function is unused in cmd after a change in datasetIngestor command
func DeleteDatasetEntry(client *http.Client, APIServer string, datasetId string, accessToken string) error {
	req, err := http.NewRequest("DELETE", APIServer+"/Datasets/"+strings.Replace(datasetId, "/", "%2F", 1)+"?access_token="+accessToken, nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return nil
}
