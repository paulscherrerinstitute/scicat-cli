package datasetUtils

import (
	"encoding/json"
	"github.com/fatih/color"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
	"time"
)

type CountResult struct {
	Count int `json:"count"`
}

func ReturnCount(client *http.Client, APIServer string, pid string, user map[string]string, collection string) (count int) {
	myurl := APIServer + "/Datasets"
	if collection != "datasets" {
		myurl += strings.Replace(pid, "/", "%2F", 1) + "/" + collection
	}
	myurl += "/count"
	req, err := http.NewRequest("GET", myurl, nil)
	req.Header.Set("Authorization", "Bearer "+user["accessToken"])
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)

	// log.Printf("response Object:\n%v\n", string(body))

	var respObj CountResult
	err = json.Unmarshal(body, &respObj)
	if err != nil {
		log.Fatal(err)
	}
	return respObj.Count
}

func RemoveFromCatalog(client *http.Client, APIServer string, pid string, user map[string]string, nonInteractive bool) {
	// first check that there are no datablocks anymore
	// check for existing OrigDatablocks, attachments first

	countOrig := ReturnCount(client, APIServer, pid, user, "origdatablocks")
	countAttachments := ReturnCount(client, APIServer, pid, user, "attachments")
	countDataset := ReturnCount(client, APIServer, pid, user, "datasets")

	if nonInteractive {
		color.Set(color.FgYellow)
		log.Printf("The dataset with pid %s will now be deleted.\n", pid)
		log.Printf("This includes the cleanup of all connected file listing blocks (%v) and attachments (%v)\n", countOrig, countAttachments)
		log.Println("You chose non-interactive mode - I will go on automatically")
		color.Unset()
	} else {
		color.Set(color.FgYellow)
		log.Printf("The dataset with pid %s will now be deleted.\n", pid)
		log.Printf("This includes the cleanup of all connected file listing blocks (%v) and attachments (%v)\n", countOrig, countAttachments)
		log.Println("Are you sure ? This action can not be undone ! Type 'y' to continue:")
		color.Unset()
		scanner.Scan()
		cont := scanner.Text()
		if cont != "y" {
			log.Fatalln("Clean up operation cancelled")
		}
	}

	// while until countDatablocks == 0
	for {
		countDatablocks := ReturnCount(client, APIServer, pid, user, "datablocks")
		if countDatablocks == 0 {
			DeleteLinkedDocuments(client, APIServer, pid, user, countOrig, countAttachments, countDataset)
			return
		} else {
			log.Println("Waiting for dataset being deleted from archiv.")
			time.Sleep(time.Second * 10)
		}
	}
}

func DeleteDocumentsFrom(collection string, client *http.Client, APIServer string, pid string, user map[string]string) {
	pidEncoded := strings.Replace(pid, "/", "%2F", 1)
	myurl := APIServer + "/Datasets/" + pidEncoded
	if collection != "datasets" {
		myurl += "/" + collection
		log.Println("Deleting linked " + collection)
	} else {
		log.Println("Deleting the dataset entry inside catalog")
	}
	req, err := http.NewRequest("DELETE", myurl, nil)
	req.Header.Set("Authorization", "Bearer "+user["accessToken"])
	resp, err := client.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()
}

func DeleteLinkedDocuments(client *http.Client, APIServer string, pid string, user map[string]string, countOrig int, countAttachments int, countDataset int) {
	if countOrig > 0 {
		DeleteDocumentsFrom("origdatablocks", client, APIServer, pid, user)
	}
	if countAttachments > 0 {
		DeleteDocumentsFrom("attachments", client, APIServer, pid, user)
	}
	if countDataset > 0 {
		DeleteDocumentsFrom("datasets", client, APIServer, pid, user)
	} else {
		color.Set(color.FgRed)
		log.Printf("The dataset %s is already removed\n", pid)
		color.Unset()
	}
}
