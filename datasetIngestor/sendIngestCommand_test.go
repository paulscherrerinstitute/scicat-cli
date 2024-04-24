package datasetIngestor

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
	"strings"
)

func TestCreateOrigBlock(t *testing.T) {
	// Define a slice of Datafile for testing
	datafiles := []Datafile{
		{Size: 100},
		{Size: 200},
		{Size: 300},
		{Size: 400},
	}
	
	// Call createOrigBlock function
	block := createOrigBlock(1, 3, datafiles, "test-dataset")
	
	// Check the Size of the returned FileBlock
	if block.Size != 500 {
		t.Errorf("Expected block size of 500, but got %d", block.Size)
	}
	
	// Check the length of DataFileList in the returned FileBlock
	if len(block.DataFileList) != 2 {
		t.Errorf("Expected 2 datafiles in the block, but got %d", len(block.DataFileList))
	}
	
	// Check the DatasetId of the returned FileBlock
	if block.DatasetId != "test-dataset" {
		t.Errorf("Expected dataset id of 'test-dataset', but got %s", block.DatasetId)
	}
}

func TestSendIngestCommand(t *testing.T) {
	// Mock HTTP client
	client := &http.Client{
		Timeout: 5 * time.Second, // Set a timeout for requests
		Transport: &http.Transport{
			// Customize the transport settings if needed (e.g., proxy, TLS config)
			// For a dummy client, default settings are usually sufficient
		},
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			// Customize how redirects are handled if needed
			// For a dummy client, default behavior is usually sufficient
			return http.ErrUseLastResponse // Use the last response for redirects
		},
	}
	
	// Mock user map
	user := map[string]string{
			"displayName": "csaxsswissfel",
			"mail":        "testuser@example.com",
			"accessToken": "test-access-token",
	}	
	
// Mock metaDataMap
	metaDataMap := map[string]interface{}{
			"accessGroups":         []string{},
			"contactEmail":         "testuser@example.com",
			"creationLocation":     "/PSI/",
			"creationTime":         "2300-01-01T11:11:11.000Z",
			"datasetName":          "CMakeCache",
			"description":          "",
			"endTime":              "2300-01-01T11:11:11.000Z",
			"owner":                "first last",
			"ownerEmail":           "test@example.com",
			"ownerGroup":           "group1",
			"principalInvestigator": "test@example.com",
			"scientificMetadata":   []map[string]map[string]string{{"sample": {"description": "", "name": "", "principalInvestigator": ""}}},
			"sourceFolder":         "/usr/share/gnome",
			"sourceFolderHost":     "PC162.psi.ch",
			"type":                 "raw",
	}
	// Mock datafiles
	datafiles := []Datafile{
		{Size: 100},
		{Size: 200},
		{Size: 300},
		{Size: 400},
	}
	
	// Create a mock server
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			// Respond with a fixed dataset ID when a new dataset is created
			if strings.HasPrefix(req.URL.Path, "/RawDatasets") || strings.HasPrefix(req.URL.Path, "/DerivedDatasets") || strings.HasPrefix(req.URL.Path, "/Datasets") {
					rw.Write([]byte(`{"pid": "test-dataset-id"}`))
			} else {
					// Respond with a 200 status code when a new data block is created
					rw.WriteHeader(http.StatusOK)
			}
	}))
	// Close the server when test finishes
	defer server.Close()
	
	// Call SendIngestCommand function with the mock server's URL and check the returned dataset ID
	datasetId := SendIngestCommand(client, server.URL, metaDataMap, datafiles, user)
	if datasetId != "test-dataset-id" {
		t.Errorf("Expected dataset id 'test-dataset-id', but got '%s'", datasetId)
	}
}
