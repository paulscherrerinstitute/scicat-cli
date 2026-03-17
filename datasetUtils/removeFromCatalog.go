package datasetUtils

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"time"

	"github.com/fatih/color"
)

type countResult struct {
	Count int `json:"count"`
}

type JobStatus string

const (
	JobSuccess JobStatus = "finishedSuccessful"
	JobFailed  JobStatus = "finishedUnsuccessful"
)

var removeFromCatalogTimeout = 5 * time.Minute
var waitTime = 10 * time.Second

func returnJobStatus(client *http.Client, APIServer string, user map[string]string, jobID string) (string, error) {
	myurl := fmt.Sprintf("%s/Jobs/%s", APIServer, url.PathEscape(jobID))

	req, err := http.NewRequest("GET", myurl, nil)
	if err != nil {
		return "", fmt.Errorf("failed to create job status request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+user["accessToken"])

	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("network error on job status: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("job status request failed (%d): %s", resp.StatusCode, string(body))
	}

	var j JobSubmissionResponse
	if err := json.NewDecoder(resp.Body).Decode(&j); err != nil {
		return "", fmt.Errorf("failed to decode job response: %w", err)
	}

	return j.JobStatusMessage, nil
}

func returnCount(client *http.Client, APIServer string, pid string, user map[string]string, collection string) (int, error) {
	myurl := APIServer + "/Datasets"
	if collection != "datasets" {
		myurl += "/" + url.PathEscape(pid) + "/" + collection
	}
	myurl += "/count"
	if collection == "datasets" {
		filter := `{"where":{"pid":"` + pid + `"}}`
		myurl += "?filter=" + url.QueryEscape(filter)
	}
	req, err := http.NewRequest("GET", myurl, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to create count request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+user["accessToken"])
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("network error on count: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return 0, fmt.Errorf("count failed with status %d: %s", resp.StatusCode, string(body))
	}

	var respObj countResult
	if err := json.NewDecoder(resp.Body).Decode(&respObj); err != nil {
		return 0, fmt.Errorf("failed to decode count response: %w", err)
	}
	return respObj.Count, nil
}

func RemoveFromCatalog(client *http.Client, APIServer string, pid string, jobID string, user map[string]string, nonInteractive bool) error {
	startTime := time.Now()
	countOrig, err := returnCount(client, APIServer, pid, user, "origdatablocks")
	if err != nil {
		return fmt.Errorf("pre-check failed: could not count origdatablocks: %w", err)
	}

	countAttachments, err := returnCount(client, APIServer, pid, user, "attachments")
	if err != nil {
		return fmt.Errorf("pre-check failed: could not count attachments: %w", err)
	}

	countDataset, err := returnCount(client, APIServer, pid, user, "datasets")
	if err != nil {
		return fmt.Errorf("pre-check failed: could not count datasets: %w", err)
	}

	color.Set(color.FgYellow)
	log.Printf("The dataset with pid %s will now be deleted.\n", pid)
	log.Printf("Blocks: %d, Attachments: %d\n", countOrig, countAttachments)

	if !nonInteractive {
		log.Println("Are you sure? This action cannot be undone! Type 'y' to continue:")
		color.Unset()
		var input string
		fmt.Scanln(&input)
		if input != "y" {
			log.Println("Cleanup operation cancelled.")
			return nil
		}
	} else {
		log.Println("Non-interactive mode: proceeding automatically.")
		color.Unset()
	}

	for {
		countDatablocks, err := returnCount(client, APIServer, pid, user, "datablocks")
		if err != nil {
			log.Printf("Error checking datablocks: %v\n", err)
		}

		jobStatus, err := returnJobStatus(client, APIServer, user, jobID)
		if err != nil {
			log.Printf("Error checking job status: %v\n", err)
		}

		if jobStatus == string(JobFailed) {
			return fmt.Errorf("archive deletion job finished with unsuccessful status")
		}

		if countDatablocks == 0 && jobStatus == string(JobSuccess) {
			err = deleteLinkedDocuments(client, APIServer, pid, user, countOrig, countAttachments, countDataset)
			if err != nil {
				return fmt.Errorf("final cleanup failed: %w", err)
			}
			return nil
		}

		if time.Since(startTime) > removeFromCatalogTimeout {
			return fmt.Errorf("timeout reached: dataset still in archive after %s", removeFromCatalogTimeout)
		}

		log.Printf("Waiting for archive deletion... (Blocks: %d)\n", countDatablocks)
		time.Sleep(waitTime)
	}
}

func deleteDocumentsFrom(collection string, client *http.Client, APIServer string, pid string, user map[string]string) error {
	pidEncoded := url.PathEscape(pid)
	myurl := APIServer + "/Datasets/" + pidEncoded
	if collection != "datasets" {
		myurl += "/" + collection
		log.Printf("Deleting linked %s...\n", collection)
	} else {
		log.Println("Deleting the primary dataset entry...")
	}
	req, err := http.NewRequest("DELETE", myurl, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+user["accessToken"])
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("delete failed (%d): %s", resp.StatusCode, string(body))
	}

	return nil
}

func deleteLinkedDocuments(client *http.Client, APIServer string, pid string, user map[string]string, countOrig int, countAttachments int, countDataset int) error {
	if countOrig > 0 {
		if err := deleteDocumentsFrom("origdatablocks", client, APIServer, pid, user); err != nil {
			return fmt.Errorf("cleanup failed at origdatablocks: %w", err)
		}
	}
	if countAttachments > 0 {
		if err := deleteDocumentsFrom("attachments", client, APIServer, pid, user); err != nil {
			return fmt.Errorf("cleanup failed at attachments: %w", err)
		}
	}
	if countDataset > 0 {
		if err := deleteDocumentsFrom("datasets", client, APIServer, pid, user); err != nil {
			color.Set(color.FgRed)
			return fmt.Errorf("cleanup failed at primary dataset: %w", err)
		}
	} else {
		color.Set(color.FgRed)
		log.Printf("The dataset %s is already removed\n", pid)
		color.Unset()
	}
	return nil
}
