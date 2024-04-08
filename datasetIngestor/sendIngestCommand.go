package datasetIngestor

import (
	"bytes"
	"encoding/json"
	"log"
	"net/http"
)

type FileBlock struct {
	Size         int64      `json:"size"`
	DataFileList []Datafile `json:"dataFileList"`
	DatasetId    string     `json:"datasetId"`
}

type payloadStruct struct {
	Text string `json:"text"`
}
type message struct {
	ShortMessage string        `json:"shortMessage"`
	Sender       string        `json:"sender"`
	Payload      payloadStruct `json:"payload"`
}

const TOTAL_MAXFILES = 400000
const BLOCK_MAXBYTES = 200000000000 // 700000 for testing the logic
const BLOCK_MAXFILES = 20000        // 20 for testing the logic

func createOrigBlock(start int, end int, filesArray []Datafile, datasetId string) (fileblock FileBlock) {
	// accumulate sizes
	var totalSize int64
	totalSize = 0
	for i := start; i < end; i++ {
		totalSize += filesArray[i].Size
	}
	// fmt.Printf("Start:%v, end:%v, totalsize:%v\n, first entry:%v\n", start, end, totalSize, filesArray[start])

	return FileBlock{Size: totalSize, DataFileList: filesArray[start:end], DatasetId: datasetId}
}

func SendIngestCommand(client *http.Client, APIServer string, metaDataMap map[string]interface{},
	fullFileArray []Datafile, user map[string]string) (datasetId string) {
	// create dataset

	cmm, _ := json.Marshal(metaDataMap)
	// metadataString := string(cmm)
	datasetId = ""

	if val, ok := metaDataMap["type"]; ok {
		dstype := val.(string)
		endpoint := ""
		if dstype == "raw" {
			endpoint = "/RawDatasets"
		} else if dstype == "derived" {
			endpoint = "/DerivedDatasets"
		} else if dstype == "base" {
			endpoint = "/Datasets"
		} else {
			log.Fatal("Unknown dataset type encountered:", dstype)
		}
		myurl := APIServer + endpoint + "/?access_token=" + user["accessToken"]
		req, err := http.NewRequest("POST", myurl, bytes.NewBuffer(cmm))
		req.Header.Set("Content-Type", "application/json")

		// fmt.Printf("request to message broker:%v\n", req)

		resp, err := client.Do(req)
		if err != nil {
			log.Fatal(err)
		}
		defer resp.Body.Close()

		// log.Println("response Status:", resp.Status)
		// fmt.Println("response Headers:", resp.Header)
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
			datasetId = d.Pid
			log.Printf("Created dataset with id %v", datasetId)
		} else {
			log.Fatalf("SendIngestCommand: Failed to create new dataset: status code %v\n", resp.StatusCode)
		}
	} else {
		log.Fatalf("No dataset type defined for dataset %v\n", metaDataMap)
	}

	// create OrigDatablocks
	// split datasets with many files into blocks, which limit number of files and size per block

	totalFiles := len(fullFileArray)

	if totalFiles > TOTAL_MAXFILES {
		log.Fatalf(
			"This datasets exceeds (%v) the maximum number of files per dataset , which can currently be handled by the archiving system (%v)\n",
			totalFiles, TOTAL_MAXFILES)
	}

	log.Printf("The dataset contains %v files. \n", totalFiles)

	end := 0
	var blockBytes int64
	for start := 0; end < totalFiles; {
		// loop over treated files until one of the limits is reached
		blockBytes = 0

		for end = start; end-start < BLOCK_MAXFILES && blockBytes < BLOCK_MAXBYTES && end < totalFiles; {
			blockBytes += fullFileArray[end].Size
			end++
			// log.Println("Inside inner loop:", start, end, blockBytes)
		}
		origBlock := createOrigBlock(start, end, fullFileArray, datasetId)

		payloadString, err := json.Marshal(origBlock)
		// log.Printf("Payload for block:%s\n", payloadString)
		myurl := APIServer + "/OrigDatablocks" + "?access_token=" + user["accessToken"]
		req, err := http.NewRequest("POST", myurl, bytes.NewBuffer(payloadString))
		req.Header.Set("Content-Type", "application/json")
		//fmt.Printf("request to message broker:%v\n", req)
		resp, err := client.Do(req)
		if err != nil {
			log.Fatal(err)
		}
		if resp.StatusCode != 200 {
			log.Fatalf("Unexpected response code %v when adding origDatablock for dataset id:%v", resp.Status, datasetId)
		}
		defer resp.Body.Close()

		// log.Println("response Status:", resp.Status)
		//fmt.Println("response Headers:", resp.Header)
		// body, _ := ioutil.ReadAll(resp.Body)
		// log.Println("Message response Body:", string(body)[0])
		log.Printf("Created file block from file %v to %v with total size of %v bytes and %v files \n", start, end-1, blockBytes, end-start)
		start = end
	}

	return datasetId
}
