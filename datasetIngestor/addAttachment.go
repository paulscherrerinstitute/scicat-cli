package datasetIngestor

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strings"
)

func ReadAndEncodeImage(attachmentFile string) (string, error) {
	imgFile, err := os.Open(attachmentFile)
	if err != nil {
		return "", err
	}
	defer imgFile.Close()
	
	// create a new buffer base on file size
	fInfo, _ := imgFile.Stat()
	var size int64 = fInfo.Size()
	buf := make([]byte, size)
	
	// read file content into buffer
	fReader := bufio.NewReader(imgFile)
	fReader.Read(buf)
	
	// convert the buffer bytes to base64 string
	imgBase64Str := base64.StdEncoding.EncodeToString(buf)
	return imgBase64Str, nil
}

func CreateMetadataMap(datasetId string, caption string, metaDataDataset map[string]interface{}, imgBase64Str string) (map[string]interface{}, error) {
	// assemble json structure
	var metaDataMap map[string]interface{}
	metaDataMap = make(map[string]interface{})
	metaDataMap["thumbnail"] = "data:image/jpeg;base64," + imgBase64Str
	metaDataMap["caption"] = caption
	metaDataMap["datasetId"] = datasetId
	if ownerGroup, ok := metaDataDataset["ownerGroup"]; ok {
		metaDataMap["ownerGroup"], _ = ownerGroup.(string)
	}
	if accessGroups, ok := metaDataDataset["accessGroups"]; ok {
		metaDataMap["accessGroups"], ok = accessGroups.([]string)
		if !ok {
			metaDataMap["accessGroups"], _ = accessGroups.([]interface{})
		}
	}
	return metaDataMap, nil
}

func AddAttachment(client *http.Client, APIServer string, datasetId string, metaDataDataset map[string]interface{}, accessToken string, attachmentFile string, caption string) {
	imgBase64Str, err := ReadAndEncodeImage(attachmentFile)
	if err != nil {
		log.Fatalf("Can not open attachment file %v \n", attachmentFile)
	}
	
	metaDataMap, err := CreateMetadataMap(datasetId, caption, metaDataDataset, imgBase64Str)
	if err != nil {
		log.Fatal("Connect serialize meta data map:", metaDataMap)
	}
	
	bm, err := json.Marshal(metaDataMap)
	if err != nil {
		log.Fatal("Connect serialize meta data map:", metaDataMap)
	}
	myurl := APIServer + "/Datasets/" + strings.Replace(datasetId, "/", "%2F", 1) + "/attachments?access_token=" + accessToken
	
	req, err := http.NewRequest("POST", myurl, bytes.NewBuffer(bm))
	if err != nil {
		log.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == 200 {
		log.Printf("Attachment file %v added to dataset  %v\n", attachmentFile, datasetId)
	} else {
		log.Fatalf("Attachment file %v could not be added to dataset  %v", attachmentFile, datasetId)
	}
}
