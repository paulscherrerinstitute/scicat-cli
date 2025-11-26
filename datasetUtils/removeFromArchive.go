package datasetUtils

import (
	"bytes"
	"encoding/json"
	"github.com/fatih/color"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"time"
)

type datablockInfo struct {
	Id   string `json:"id"`
	Size int    `json:"size"`
}
type queryDatablockResult []datablockInfo

type datasetStruct struct {
	Pid   string   `json:"pid"`
	Files []string `json:"files"`
}

type jobparamsStruct struct {
	Username string `json:"username"`
}

func RemoveFromArchive(client *http.Client, APIServer string, pid string, user map[string]string, nonInteractive bool) {
	// check for existing Datablocks first
	var respObj = getDatablocks(client, APIServer, pid, user)

	// TODO add printout of datasets sourceFolder
	if len(respObj) == 0 {
		color.Set(color.FgGreen)
		log.Println("No datablocks found for this dataset - dataset already cleaned from archive")
		color.Unset()
		return
	}
	log.Printf("Found the following datablocks for dataset %s", pid)
	var item datablockInfo
	for _, item = range respObj {
		log.Printf("Id %v, size: %v", item.Id, item.Size)
	}
	// Set up reset job
	log.Println("Setting up reset job to remove dataset inside archive system")
	color.Set(color.FgYellow)
	if nonInteractive {
		log.Println("You chose the non interactive flag - I will go on automatically.")
		color.Unset()
	} else {
		log.Println("Are you sure ? This action can not be undone ! Type 'y' to continue.")
		color.Unset()
		scanner.Scan()
		cont := scanner.Text()
		if cont != "y" {
			log.Fatalln("Clean up operation cancelled")
		}
	}

	var jobMap = buildResetJobMap(pid, user)
	submitJob(client, APIServer, user, jobMap)
}

func getDatablocks(client *http.Client, APIServer string, pid string, user map[string]string) queryDatablockResult {
	filter := `{"where":{"datasetId":"` + pid + `"},"fields": {"id":1,"size":1}}`
	url := APIServer + "/Datablocks?filter=" + url.QueryEscape(filter)

	req, err := http.NewRequest("GET", url, nil)
	req.Header.Set("Authorization", "Bearer "+ user["accessToken"])
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)

	var respObj queryDatablockResult
	err = json.Unmarshal(body, &respObj)
	if err != nil {
		log.Fatal(err)
	}
	return respObj
}

func buildResetJobMap(pid string, user map[string]string) map[string]interface{} {
	jobMap := make(map[string]interface{})
	jobMap["emailJobInitiator"] = user["mail"]
	jobMap["type"] = "reset"
	jobMap["creationTime"] = time.Now().Format(time.RFC3339)
	// TODO these job parameters may become obsolete
	jobMap["jobParams"] = jobparamsStruct{user["username"]}
	jobMap["jobStatusMessage"] = "jobSubmitted"

	emptyfiles := make([]string, 0)

	var dsMap []datasetStruct
	dsMap = append(dsMap, datasetStruct{pid, emptyfiles})
	jobMap["datasetList"] = dsMap

	return jobMap
}

func submitJob(client *http.Client, APIServer string, user map[string]string, jobMap map[string]interface{}) {
	var bmm []byte
	bmm, _ = json.Marshal(jobMap)

	// now send  archive job request
	myurl := APIServer + "/Jobs"
	req, err := http.NewRequest("POST", myurl, bytes.NewBuffer(bmm))
	req.Header.Set("Authorization", "Bearer "+ user["accessToken"])
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	body, _ := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Println("response Body:", string(body))
		log.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == 200 {
		log.Println("Job response Status: okay")
		log.Println("A confirmation email will be sent to", user["mail"])
	} else {
		log.Println("Job response Status: there are problems:", resp.StatusCode)
		log.Fatalln("Job response Body:", string(body))
	}
}
