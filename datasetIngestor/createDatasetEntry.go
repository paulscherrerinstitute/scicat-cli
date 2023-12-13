package datasetIngestor

import (
	"bytes"
	"encoding/json"
	"log"
	"net/http"
)

func CreateDatasetEntry(client *http.Client, APIServer string, metaDataMap map[string]interface{}, accessToken string) (datasetId string) {
	// assemble json structure
	bm, err := json.Marshal(metaDataMap)
	if err != nil {
		log.Fatal("Connect serialize meta data map:", metaDataMap)
	}
	if val, ok := metaDataMap["type"]; ok {
		dstype := val.(string)
		// fmt.Println(errm,sourceFolder)

		// verify data structure of meta data by calling isValid API for Dataset

		myurl := ""
		if dstype == "raw" {
			myurl = APIServer + "/RawDatasets"
		} else if dstype == "derived" {
			myurl = APIServer + "/DerivedDatasets"
		} else if dstype == "base" {
			myurl = APIServer + "/Datasets"
		} else {
			log.Fatal("Unknown dataset type encountered:", dstype)
		}

		req, err := http.NewRequest("POST", myurl+"?access_token="+accessToken, bytes.NewBuffer(bm))
		req.Header.Set("Content-Type", "application/json")
		//fmt.Printf("request to message broker:%v\n", req)
		resp, err := client.Do(req)
		if err != nil {
			log.Fatal(err)
		}
		defer resp.Body.Close()
		if resp.StatusCode == 200 {
			// important: use capital first character in field names!
			type PidType struct {
				Pid string `json:"pid"`
			}
			decoder := json.NewDecoder(resp.Body)
			var d PidType
			err := decoder.Decode(&d)
			if err != nil {
				log.Fatal("Could not decode pid from dataset entry:", err)
			}
			// fmtlog.Printf("Extracted pid:%s", d.Pid)
			return d.Pid
		} else {
			log.Fatalf("CreateDatasetEntry:Failed to create new dataset: status code %v\n", resp.StatusCode)
		}
	} else {
		log.Fatal("Type of dataset not defined:")
	}
	return "This should never happen"
}
