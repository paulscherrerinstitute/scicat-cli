package datasetUtils

import (
	"bytes"
	"encoding/json"
	"log"
	"net/http"
	"time"
)

func CreateRetrieveJob(client *http.Client, APIServer string, user map[string]string, datasetList []string) (jobId string) {

	// important: define field with capital names and rename fields via 'json' constructs
	// otherwise the marshaling will omit the fields !

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
			log.Fatal("Could not decode id from job:", err)
		}
		return j.Id
	} else {
		log.Println("Job response Status: there are problems:", resp.StatusCode)
		return ""
	}
}
