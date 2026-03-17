package datasetUtils

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"time"

	"github.com/fatih/color"
)

type datablockInfo struct {
	ID   string `json:"id"`
	Size int    `json:"size"`
}

type datasetStruct struct {
	Pid   string   `json:"pid"`
	Files []string `json:"files"`
}

type jobParamsStruct struct {
	Username string `json:"username"`
}

type JobSubmissionResponse struct {
	ID string `json:"id"`
	JobStatusMessage string `json:"jobStatusMessage"`
}

func RemoveFromArchive(client *http.Client, APIServer string, pid string, user map[string]string, nonInteractive bool) (string, error) {
	respObj, err := getDatablocks(client, APIServer, pid, user)
	if err != nil {
		return "", fmt.Errorf("failed to fetch datablocks: %w", err)
	}

	if len(respObj) == 0 {
		color.Set(color.FgGreen)
		log.Println("No datablocks found - dataset already cleaned from archive.")
		color.Unset()
		return "", nil
	}

	log.Printf("Found %d datablocks for dataset %s", len(respObj), pid)
	for _, item := range respObj {
		log.Printf("ID: %s, Size: %d", item.ID, item.Size)
	}
	// Set up reset job
	log.Println("Setting up reset job to remove dataset inside archive system")
	if !nonInteractive {
		color.Set(color.FgYellow)
		log.Println("Are you sure? This action cannot be undone! Type 'y' to continue:")
		color.Unset()
		var input string
		fmt.Scanln(&input)
		if input != "y" {
			return "", fmt.Errorf("clean up operation cancelled by user")
		}
	} else {
		log.Println("Non-interactive mode: proceeding automatically.")
	}
	jobMap := buildResetJobMap(pid, user)
	jobID, err := submitJob(client, APIServer, user, jobMap)
	if err != nil {
		return "", fmt.Errorf("archive reset job submission failed: %w", err)
	}

	return jobID, nil
}

func getDatablocks(client *http.Client, APIServer string, pid string, user map[string]string) ([]datablockInfo, error) {
	filter := fmt.Sprintf(`{"where":{"datasetId":"%s"},"fields": {"id":1,"size":1}}`, pid)
	url := fmt.Sprintf("%s/Datablocks?filter=%s", APIServer, url.QueryEscape(filter))

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+user["accessToken"])
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("API error %d: %s", resp.StatusCode, string(body))
	}

	var respObj []datablockInfo
	if err := json.NewDecoder(resp.Body).Decode(&respObj); err != nil {
		return nil, fmt.Errorf("failed to decode datablocks: %w", err)
	}
	return respObj, nil
}

func buildResetJobMap(pid string, user map[string]string) map[string]interface{} {
	return map[string]interface{}{
		"emailJobInitiator": user["mail"],
		"type":              "reset",
		"creationTime":      time.Now().Format(time.RFC3339),
		"jobParams":         jobParamsStruct{Username: user["username"]},
		"jobStatusMessage":  "jobSubmitted",
		"datasetList": []datasetStruct{
			{Pid: pid, Files: []string{}},
		},
	}
}

func submitJob(client *http.Client, APIServer string, user map[string]string, jobMap map[string]interface{}) (string, error) {
	jsonData, err := json.Marshal(jobMap)
	if err != nil {
		return "", fmt.Errorf("json marshal failed: %w", err)
	}

	myurl := APIServer + "/Jobs"
	req, err := http.NewRequest("POST", myurl, bytes.NewBuffer(jsonData))
	if err != nil {
		return "", fmt.Errorf("failed to create job request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+user["accessToken"])
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("network error on job submission: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("job submission failed (%d): %s", resp.StatusCode, string(body))
	}

	var respObj JobSubmissionResponse
	if err := json.NewDecoder(resp.Body).Decode(&respObj); err != nil {
		return "", fmt.Errorf("failed to decode job submission response: %w", err)
	}

	if respObj.ID == "" {
		return "", fmt.Errorf("job submission response missing job ID")
	}

	log.Println("Job response Status: okay")
	log.Println("A confirmation email will be sent to", user["mail"])
	return respObj.ID, nil
}
