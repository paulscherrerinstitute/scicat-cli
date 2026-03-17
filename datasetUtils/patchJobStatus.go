package datasetUtils

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
)

func PatchJobStatus(client *http.Client, APIServer string, user map[string]string, jobID string, status string) error {
	myurl := fmt.Sprintf("%s/Jobs/%s", APIServer, url.PathEscape(jobID))
	payload := map[string]string{
		"jobStatusMessage": status,
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal patch payload: %w", err)
	}
	req, err := http.NewRequest("PATCH", myurl, bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("failed to create job status request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+user["accessToken"])
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("network error on job status: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("job status request failed (%d): %s", resp.StatusCode, string(body))
	}

	return nil
}
