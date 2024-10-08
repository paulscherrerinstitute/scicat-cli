package datasetUtils

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"
)

func constructJobRequest(user map[string]string, datasetList []string) ([]byte, error) {
	type datasetStruct struct {
		Pid   string   `json:"pid"`
		Files []string `json:"files"`
	}

	type jobparamsStruct struct {
		DestinationPath string `json:"destinationPath"`
		Username        string `json:"username"`
	}

	jobMap := make(map[string]interface{})
	jobMap["emailJobInitiator"] = user["mail"]
	jobMap["type"] = "retrieve"
	jobMap["creationTime"] = time.Now().Format(time.RFC3339)
	jobMap["jobParams"] = jobparamsStruct{"/archive/retrieve", user["username"]}
	jobMap["jobStatusMessage"] = "jobSubmitted"

	emptyfiles := make([]string, 0)

	dsMap := make([]datasetStruct, len(datasetList))
	for i, dataset := range datasetList {
		dsMap[i] = datasetStruct{dataset, emptyfiles}
	}
	jobMap["datasetList"] = dsMap

	// marshal to JSON
	return json.Marshal(jobMap)
}

func sendJobRequest(client *http.Client, APIServer string, user map[string]string, bmm []byte) (*http.Response, error) {
	myurl := APIServer + "/Jobs?access_token=" + user["accessToken"]
	req, err := http.NewRequest("POST", myurl, bytes.NewBuffer(bmm))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func handleJobResponse(resp *http.Response, user map[string]string) (string, error) {
	if resp.StatusCode == 200 {
		log.Println("Job response Status: okay")
		log.Println("A confirmation email will be sent to", user["mail"])
		decoder := json.NewDecoder(resp.Body)
		var j Job
		err := decoder.Decode(&j)
		if err != nil {
			return "", fmt.Errorf("could not decode id from job: %v", err)
		}
		return j.Id, nil
	} else {
		log.Println("Job response Status: there are problems:", resp.StatusCode)
		return "", fmt.Errorf("Job response Status: there are problems: %d", resp.StatusCode)
	}
}

/*
CreateRetrieveJob creates a job to retrieve a dataset from an API server.

Parameters:
- client: An *http.Client object that is used to send the HTTP request.
- APIServer: A string representing the URL of the API server.
- user: A map[string]string containing user information. It should have keys "mail", "username", and "accessToken".
- datasetList: A slice of strings representing the list of datasets to be retrieved.

The function constructs a job request with the provided parameters and sends it to the API server. If the job is successfully created, it returns the job ID as a string. If the job creation fails, it returns an empty string.

The function logs the status of the job creation and sends a confirmation email to the user if the job is successfully created.

Note: The function will terminate the program if it encounters an error while sending the HTTP request or decoding the job ID from the response.
*/
func CreateRetrieveJob(client *http.Client, APIServer string, user map[string]string, datasetList []string) (jobId string, err error) {
	bmm, err := constructJobRequest(user, datasetList)
	if err != nil {
		return "", err
	}

	resp, err := sendJobRequest(client, APIServer, user, bmm)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	jobId, err = handleJobResponse(resp, user)
	if err != nil {
		return "", err
	}

	return jobId, nil
}
