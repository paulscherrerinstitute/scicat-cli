package datasetUtils

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
)

func PatchDataset(client *http.Client, APIServer string, token string, datasetId string, meta map[string]interface{}) error {
	cmm, _ := json.Marshal(meta)

	myurl := APIServer + "/Datasets/" + url.QueryEscape(datasetId)
	req, err := http.NewRequest("PATCH", myurl, bytes.NewBuffer(cmm))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 || resp.StatusCode < 200 {
		return fmt.Errorf("failed to update dataset %v %v", resp.StatusCode, meta)
	}
	return nil
}
