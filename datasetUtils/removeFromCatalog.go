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

func returnCount(client *http.Client, APIServer string, pid string, user map[string]string, collection string) (count int) {
	myurl := ""
	if collection == "datasets" {
		myurl = APIServer + "/Datasets/count?access_token=" + user["accessToken"]
	} else {
		myurl = APIServer + "/Datasets/" + strings.Replace(pid, "/", "%2F", 1) + "/" + collection + "/count?access_token=" + user["accessToken"]
	}
	req, err := http.NewRequest("GET", myurl, nil)
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

func RemoveFromCatalog(client *http.Client, APIServer string, pid string, user map[string]string, nonInteractive bool, waitSeconds time.Duration) {
	// first check that there are no datablocks anymore
	// check for existing OrigDatablocks, attachments first
	
	countOrig := returnCount(client, APIServer, pid, user, "origdatablocks")
	countAttachments := returnCount(client, APIServer, pid, user, "attachments")
	countDataset := returnCount(client, APIServer, pid, user, "datasets")

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

	pidEncoded := strings.Replace(pid, "/", "%2F", 1)

	for {
		countDatablocks := returnCount(client, APIServer, pid, user, "datablocks")
		if countDatablocks == 0 {
			if countOrig > 0 {
				log.Println("Deleting blocks containing filelistings")
				req, err := http.NewRequest("DELETE", APIServer+"/Datasets/"+pidEncoded+"/origdatablocks?access_token="+user["accessToken"], nil)
				resp, err := client.Do(req)
				if err != nil {
					log.Fatal(err)
				}
				defer resp.Body.Close()
			}
			if countAttachments > 0 {
				log.Println("Deleting linked attachments")
				req, err := http.NewRequest("DELETE", APIServer+"/Datasets/"+pidEncoded+"/attachments?access_token="+user["accessToken"], nil)
				resp, err := client.Do(req)
				if err != nil {
					log.Fatal(err)
				}
				defer resp.Body.Close()
			}
			if countDataset > 0 {
				log.Println("Deleting the dataset entry inside catalog")
				req, err := http.NewRequest("DELETE", APIServer+"/Datasets/"+pidEncoded+"?access_token="+user["accessToken"], nil)
				resp, err := client.Do(req)
				if err != nil {
					log.Fatal(err)
				}
				defer resp.Body.Close()
			} else {
				color.Set(color.FgRed)
				log.Printf("The dataset %s is already removed\n", pid)
				color.Unset()
			}
			return
		} else {
			log.Println("Waiting for dataset being deleted from archiv.")
			time.Sleep(time.Second * waitSeconds)
		}
	}
}
