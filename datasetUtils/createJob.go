package datasetUtils

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

type Job struct {
	Id string `json:"id"`
}

/*
`CreateArchivalJob` creates a new job on the server. It takes in an HTTP client, the API server URL, a user map, a list of datasets, and a pointer to an integer representing the number of tape copies.

The function constructs a job map with various parameters, including the email of the job initiator, the type of job, the creation time, the job parameters, and the job status message. It also includes a list of datasets.

The job map is then marshalled into JSON and sent as a POST request to the server. If the server responds with a status code of 200, the function decodes the job ID from the response and returns it. If the server responds with any other status code, the function returns an empty string.

Note that the job will belong to one specific ownerGroup. Use CreateArchivalJobs to create a job per ownergroup.

Parameters:
- client: A pointer to an http.Client instance
- APIServer: A string representing the API server URL
- user: A map with string keys and values representing user information
- datasetMap: A list of datasets grouped by ownerGroups
- tapecopies: A pointer to an integer representing the number of tape copies

Returns:
- jobId: A string representing the job ID if the job was successfully created, or an empty string otherwise
*/
func CreateArchivalJob(client *http.Client, APIServer string, user map[string]string, ownerGroup string, datasetList []string, tapecopies *int) (jobId string, err error) {
	// important: define field with capital names and rename fields via 'json' constructs
	// otherwise the marshaling will omit the fields !

	type datasetStruct struct {
		Pid   string   `json:"pid"`
		Files []string `json:"files"`
	}

	type jobParamsStruct struct {
		TapeCopies  string          `json:"tapeCopies"`
		Username    string          `json:"username"`
		DatasetList []datasetStruct `json:"datasetList"`
	}

	type createJobDto struct {
		JobType      string          `json:"type"`
		JobParams    jobParamsStruct `json:"jobParams"`
		OwnerUser    string          `json:"ownerUser"`
		OwnerGroup   string          `json:"ownerGroup"`
		ContactEmail string          `json:"contactEmail"`
	}

	if ownerGroup == "" {
		return "", fmt.Errorf("no owner group was specified")
	}

	//jobMap["creationTime"] = time.Now().Format(time.RFC3339)
	// TODO these job parameters may become obsolete
	tc := "one"
	if *tapecopies == 2 {
		tc = "two"
	}

	emptyfiles := []string{}
	dsMap := make([]datasetStruct, len(datasetList))
	for i, dataset := range datasetList {
		dsMap[i] = datasetStruct{dataset, emptyfiles}
	}

	// TODO how the heck can I add a dataset list with the new format?
	// jobMap["datasetList"] = dsMap
	createJob := createJobDto{
		JobType: "archive",
		JobParams: jobParamsStruct{
			TapeCopies:  tc,
			Username:    user["username"],
			DatasetList: dsMap,
		},
		OwnerUser:    user["username"],
		OwnerGroup:   ownerGroup,
		ContactEmail: user["mail"],
	}

	// marshal to JSON
	bmm, err := json.Marshal(createJob)
	if err != nil {
		return "", err
	}
	// fmt.Printf("Marshalled job description : %s\n", string(bmm))

	// now send  archive job request
	myurl := APIServer + "/jobs"
	req, err := http.NewRequest("POST", myurl, bytes.NewBuffer(bmm))
	req.Header.Set("Authorization", "Bearer "+user["accessToken"])
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return "", fmt.Errorf("CreateJob - request returned error status code: %d", resp.StatusCode)
		}
		return "", fmt.Errorf("CreateJob - request returned error status code: %d, body: %s", resp.StatusCode, string(body))
	}
	// the request succeeded based on status code
	// an email should be sent by SciCat to user["email"]
	decoder := json.NewDecoder(resp.Body)
	var j Job
	err = decoder.Decode(&j)
	if err != nil {
		return "", fmt.Errorf("CreateJob - could not decode id from job: %v", err)
	}
	return j.Id, err
}

// Auxiliary function to CreateArchivalJob when you need to use a list of datasets grouped by ownerGroups
func CreateArchivalJobs(client *http.Client, APIServer string, user map[string]string, groupedDatasetLists map[string][]string, tapecopies *int) (jobIds []string, errs []error) {
	jobIds = make([]string, len(groupedDatasetLists))
	errs = make([]error, len(groupedDatasetLists))
	i := 0
	for group := range groupedDatasetLists {
		jobIds[i], errs[i] = CreateArchivalJob(client, APIServer, user, group, groupedDatasetLists[group], tapecopies)
	}
	return jobIds, errs
}

// This function creates a map that groups datasets by ownerGroups
func GroupDatasetsByOwnerGroup(datasetList []string, ownerGroupList []string) (groupedDatasets map[string][]string, err error) {
	if len(datasetList) != len(ownerGroupList) {
		return nil, fmt.Errorf("datasetList and ownerGroupList are not the same length")
	}

	groupedDatasets = make(map[string][]string)

	for _, ownerGroup := range ownerGroupList {
		groupedDatasets[ownerGroup] = []string{}
	}

	for ownerGroup := range groupedDatasets {
		var currList []string
		for i, currentOwnerGroup := range ownerGroupList {
			if ownerGroup == currentOwnerGroup {
				currList = append(currList, datasetList[i])
			}
		}
		groupedDatasets[ownerGroup] = currList
	}

	return groupedDatasets, nil
}
