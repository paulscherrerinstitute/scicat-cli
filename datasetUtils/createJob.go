package datasetUtils

import (
	"bytes"
	"encoding/json"
	"log"
	"net/http"
	"time"
)

type Job struct {
	Id string `json:"id"`
}

/*
`CreateJob` creates a new job on the server. It takes in an HTTP client, the API server URL, a user map, a list of datasets, and a pointer to an integer representing the number of tape copies. 

The function constructs a job map with various parameters, including the email of the job initiator, the type of job, the creation time, the job parameters, and the job status message. It also includes a list of datasets. 

The job map is then marshalled into JSON and sent as a POST request to the server. If the server responds with a status code of 200, the function decodes the job ID from the response and returns it. If the server responds with any other status code, the function returns an empty string.

Parameters:
- client: A pointer to an http.Client instance
- APIServer: A string representing the API server URL
- user: A map with string keys and values representing user information
- datasetList: A slice of strings representing the list of datasets
- tapecopies: A pointer to an integer representing the number of tape copies

Returns:
- jobId: A string representing the job ID if the job was successfully created, or an empty string otherwise
*/
func CreateJob(client *http.Client, APIServer string, user map[string]string, datasetList []string, tapecopies *int) (jobId string) {
	// important: define field with capital names and rename fields via 'json' constructs
	// otherwise the marshaling will omit the fields !

	type datasetStruct struct {
		Pid   string   `json:"pid"`
		Files []string `json:"files"`
	}

	type jobparamsStruct struct {
		TapeCopies string `json:"tapeCopies"`
		Username   string `json:"username"`
	}

	jobMap := make(map[string]interface{})
	jobMap["emailJobInitiator"] = user["mail"]
	jobMap["type"] = "archive"
	jobMap["creationTime"] = time.Now().Format(time.RFC3339)
	// TODO these job parameters may become obsolete
	tc := "one"
	if *tapecopies == 2 {
		tc = "two"
	}
	jobMap["jobParams"] = jobparamsStruct{tc, user["username"]}
	jobMap["jobStatusMessage"] = "jobSubmitted"

	emptyfiles := make([]string, 0)

	var dsMap []datasetStruct
	for i := 0; i < len(datasetList); i++ {
		dsMap = append(dsMap, datasetStruct{datasetList[i], emptyfiles})
	}
	jobMap["datasetList"] = dsMap

	// marshal to JSON
	var bmm []byte
	bmm, _ = json.Marshal(jobMap)
	// fmt.Printf("Marshalled job description : %s\n", string(bmm))

	// now send  archive job request
	myurl := APIServer + "/Jobs?access_token=" + user["accessToken"]
	req, err := http.NewRequest("POST", myurl, bytes.NewBuffer(bmm))
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == 200 {
		log.Println("Job response Status: okay")
		log.Println("A confirmation email will be sent to", user["mail"])
		decoder := json.NewDecoder(resp.Body)
		var j Job
		err := decoder.Decode(&j)
		if err != nil {
			log.Println("Could not decode id from job:", err)
			return ""
		}
		return j.Id
	} else {
		log.Println("Job response Status: there are problems:", resp.StatusCode)
		return ""
	}
}
