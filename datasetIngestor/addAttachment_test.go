package datasetIngestor

import (
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
)

// Check whether the function is called without a panic
func TestAddAttachment(t *testing.T) {
	// Create a mock server
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer mockServer.Close()

	client := &http.Client{}
	APIServer := mockServer.URL
	datasetId := "testDatasetId"
	metaDataDataset := make(map[string]interface{})
	accessToken := "testAccessToken"
	caption := "testCaption"

	// Create a temporary file
	tempFile, err := os.CreateTemp("", "testAttachmentFile")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tempFile.Name()) // clean up

	attachmentFile := tempFile.Name()

	defer func() {
		if r := recover(); r != nil {
			t.Errorf("The code panicked with %v", r)
		}
	}()

	err = AddAttachment(client, APIServer, datasetId, metaDataDataset, accessToken, attachmentFile, caption)
	if err != nil {
		t.Errorf("The function returned an error: \"%v\"", err)
	}
}

func TestReadAndEncodeImage(t *testing.T) {
	// Create a temporary file
	tempFile, err := os.CreateTemp("", "testImageFile.jpg")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tempFile.Name()) // clean up

	imageFile := tempFile.Name()

	encodedStr, err := ReadAndEncodeImage(imageFile)
	if err != nil {
		t.Errorf("ReadAndEncodeImage returned an error: %v", err)
	}

	// Check if the output is a valid base64 string
	_, err = base64.StdEncoding.DecodeString(encodedStr)
	if err != nil {
		t.Errorf("ReadAndEncodeImage returned an invalid base64 string: %v", err)
	}
}

func TestCreateAttachmentMap(t *testing.T) {
	datasetId := "testDatasetId"
	caption := "testCaption"
	attachmentMap := make(map[string]interface{})

	// Create a temporary file
	tempFile, err := os.CreateTemp("", "testImageFile")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tempFile.Name()) // clean up

	imageFile := tempFile.Name()

	imgBase64Str, err := ReadAndEncodeImage(imageFile)
	if err != nil {
		t.Fatal(err)
	}

	metaDataMap, err := CreateAttachmentMap(datasetId, caption, attachmentMap, imgBase64Str)
	if err != nil {
		t.Errorf("CreateMetadataMap returned an error: %v", err)
	}

	// Check if the map contains the correct keys and values
	if metaDataMap["thumbnail"] != "data:image/jpeg;base64,"+imgBase64Str {
		t.Errorf("Incorrect thumbnail: got %v, want %v", metaDataMap["thumbnail"], "data:image/jpeg;base64,"+imgBase64Str)
	}

	if metaDataMap["caption"] != caption {
		t.Errorf("Incorrect caption: got %v, want %v", metaDataMap["caption"], caption)
	}

	if metaDataMap["datasetId"] != datasetId {
		t.Errorf("Incorrect datasetId: got %v, want %v", metaDataMap["datasetId"], datasetId)
	}

	// Check if the map does not contain the keys "ownerGroup" and "accessGroups"
	if _, ok := metaDataMap["ownerGroup"]; ok {
		t.Errorf("Map contains unexpected key: ownerGroup")
	}

	if _, ok := metaDataMap["accessGroups"]; ok {
		t.Errorf("Map contains unexpected key: accessGroups")
	}
}
