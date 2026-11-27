package datasetUtils

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestGetDatasetDetails_EmptyList(t *testing.T) {
	// Create a mock server
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		// Send response to be tested
		rw.Write([]byte(`[]`))
	}))
	// Close the server when test finishes
	defer server.Close()

	// Use the mock server's URL as the API
	APIServer := server.URL
	accessToken := "testToken"
	datasetList := []string{}
	ownerGroup := "group1"

	// Create a new HTTP client
	client := &http.Client{}

	// Call the function to be tested
	datasets, notFoundIds, err := GetDatasetDetails(client, APIServer, accessToken, datasetList, ownerGroup)
	if err != nil {
		t.Errorf("Unexpected error: %s", err.Error())
	}

	// Check the result
	if len(datasets) != 0 {
		t.Errorf("Expected 0 datasets, got %d", len(datasets))
	}
	if len(notFoundIds) != 0 {
		t.Errorf("")
	}
}

func TestGetDatasetDetails_Non200StatusCode(t *testing.T) {
	// Create a mock server
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		// Send a non-200 status code
		rw.WriteHeader(http.StatusNotFound)
	}))
	// Close the server when test finishes
	defer server.Close()

	// Use the mock server's URL as the API
	APIServer := server.URL
	accessToken := "testToken"
	datasetList := []string{"123"}
	ownerGroup := "group1"

	// Create a new HTTP client
	client := &http.Client{}

	// Call the function to be tested
	datasets, notFoundIds, err := GetDatasetDetails(client, APIServer, accessToken, datasetList, ownerGroup)
	if err == nil {
		t.Errorf("Expected an error to be returned, got nil")
	}

	// Check the result
	if len(datasets) != 0 {
		t.Errorf("Expected 0 datasets, got %d", len(datasets))
	}
	if len(notFoundIds) != 0 {
		t.Errorf("Expected 0 not found IDs, got %d", len(notFoundIds))
	}
}

func TestGetDatasetDetails_DatasetNotFound(t *testing.T) {
	// Create a mock server
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		// Send response to be tested
		rw.Write([]byte(`[]`))
	}))
	// Close the server when test finishes
	defer server.Close()

	// Use the mock server's URL as the API
	APIServer := server.URL
	accessToken := "testToken"
	datasetList := []string{"123"}
	ownerGroup := "group1"

	// Create a new HTTP client
	client := &http.Client{}

	// Call the function to be tested
	datasets, notFoundIds, err := GetDatasetDetails(client, APIServer, accessToken, datasetList, ownerGroup)
	if err != nil {
		t.Errorf("Unexpected error: %s", err.Error())
	}

	// Check the result
	if len(datasets) != 0 {
		t.Errorf("Expected 0 datasets, got %d", len(datasets))
	}
	if len(notFoundIds) != 1 {
		t.Errorf("Expected 1 ID that was not found, got %d", len(notFoundIds))
	}
}

func TestGetDatasetDetails_DatasetFound(t *testing.T) {
	// Create a mock server
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		// Send response to be tested
		rw.Write([]byte(`[{"Pid":"123","SourceFolder":"/path/to/dataset","Size":1024,"OwnerGroup":"group1","NumberOfFiles":10}]`))
	}))
	// Close the server when test finishes
	defer server.Close()

	// Use the mock server's URL as the API
	APIServer := server.URL
	accessToken := "testToken"
	datasetList := []string{"123"}
	ownerGroup := "group1"

	// Create a new HTTP client
	client := &http.Client{}

	// Call the function to be tested
	datasets, notFoundIds, err := GetDatasetDetails(client, APIServer, accessToken, datasetList, ownerGroup)
	if err != nil {
		t.Errorf("Unexpected error: %s", err.Error())
	}

	// Check the result
	if len(datasets) != 1 {
		t.Errorf("Expected 1 dataset, got %d", len(datasets))
	} else {
		dataset := datasets[0]
		if dataset.Pid != "123" || dataset.SourceFolder != "/path/to/dataset" || dataset.Size != 1024 || dataset.OwnerGroup != "group1" || dataset.NumberOfFiles != 10 {
			t.Errorf("Dataset details do not match expected values")
		}
	}
	if len(notFoundIds) != 0 {
		t.Errorf("Expected no IDs that couldn't be found, got %d", len(notFoundIds))
	}
}

func TestGetDatasetDetails_FilterString(t *testing.T) {
	accessToken := "testToken"
	datasetList := []string{"123"}
	ownerGroup := "group1"
	expectedFilter := `{"where":{"pid":{"inq":["` + strings.Join(datasetList, `","`) + `"]},"ownerGroup":"` + ownerGroup + `"},"fields":{"pid":true,"sourceFolder":true,"size":true,"ownerGroup":true}}`

	// Create a mock server
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		filter := req.URL.Query().Get("filter")
		if filter != expectedFilter {
			t.Errorf("Filter does not match expected value - got: \"%s\", expected: \"%s\"", filter, expectedFilter)
		}
		// Send some response
		rw.Write([]byte(`[{"Pid":"123","SourceFolder":"/path/to/dataset","Size":1024,"OwnerGroup":"group1","NumberOfFiles":10}]`))
	}))
	// Close the server when test finishes
	defer server.Close()
	APIServer := server.URL

	// Create a new HTTP client
	client := &http.Client{}

	// Call the function to be tested
	GetDatasetDetails(client, APIServer, accessToken, datasetList, ownerGroup)
}
