package datasetIngestor

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestCreateDatasetEntry(t *testing.T) {
	var capturedPath string

	// Create a mock server
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		rw.Write([]byte(`{"pid": "1234"}`))
		capturedPath = req.URL.Path
	}))
	// Close the server when test finishes
	defer server.Close()

	// Create a map for the metaData
	metaDataMap := map[string]interface{}{
		"type": "raw",
	}

	// Create a client
	client := server.Client()

	// Call the function
	datasetId, err := CreateDatasetEntry(client, server.URL, metaDataMap, "testToken")
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	// Check the returned datasetId
	if datasetId != "1234" {
		t.Errorf("Expected datasetId to be '1234', but got '%s'", datasetId)
	}

	// Check the URL based on the type field
	expectedPath := ""
	switch metaDataMap["type"].(string) {
	case "raw":
		expectedPath = "/RawDatasets"
	case "derived":
		expectedPath = "/DerivedDatasets"
	case "base":
		expectedPath = "/Datasets"
	}

	// Check the URL
	if capturedPath != expectedPath {
		t.Errorf("Expected path to be '%s', but got '%s'", expectedPath, capturedPath)
	}
}
