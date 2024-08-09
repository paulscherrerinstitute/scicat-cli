package datasetIngestor

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
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
	fInfo, err := imgFile.Stat()
	if err != nil {
		return "", err
	}
	var size int64 = fInfo.Size()
	buf := make([]byte, size)

	// read file content into buffer
	fReader := bufio.NewReader(imgFile)
	_, err = fReader.Read(buf)
	if err != nil {
		return "", err
	}

	// convert the buffer bytes to base64 string
	imgBase64Str := base64.StdEncoding.EncodeToString(buf)
	return imgBase64Str, nil
}

func CreateAttachmentMap(datasetId string, caption string, datasetMetadata map[string]interface{}, imgBase64Str string) (map[string]interface{}, error) {
	// assemble json structure
	metadata := make(map[string]interface{})
	metadata["thumbnail"] = "data:image/jpeg;base64," + imgBase64Str
	metadata["caption"] = caption
	metadata["datasetId"] = datasetId
	// if we're able, extract some informations from the dataset metadata
	if ownerGroup, ok := datasetMetadata["ownerGroup"]; ok {
		metadata["ownerGroup"], _ = ownerGroup.(string)
	}
	if accessGroups, ok := datasetMetadata["accessGroups"]; ok {
		if metadata["accessGroups"], ok = accessGroups.([]string); !ok {
			metadata["accessGroups"] = accessGroups // fallback (might fail at JSON conversion later)
		}
	}
	return metadata, nil
}

func AddAttachment(client *http.Client, APIServer string, datasetId string, datasetMetadata map[string]interface{}, accessToken string, attachmentFile string, caption string) error {
	imgBase64Str, err := ReadAndEncodeImage(attachmentFile)
	if err != nil {
		return err
	}

	attachmentMap, err := CreateAttachmentMap(datasetId, caption, datasetMetadata, imgBase64Str)
	if err != nil {
		return err
	}

	attachmentJson, err := json.Marshal(attachmentMap)
	if err != nil {
		return err
	}
	myurl := APIServer + "/Datasets/" + strings.Replace(datasetId, "/", "%2F", 1) + "/attachments?access_token=" + accessToken

	req, err := http.NewRequest("POST", myurl, bytes.NewBuffer(attachmentJson))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("attachment file %v could not be added to dataset %v - status code: %d", attachmentFile, datasetId, resp.StatusCode)
	}
	return nil
}
