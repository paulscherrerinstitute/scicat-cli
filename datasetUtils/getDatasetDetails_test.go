package datasetUtils

import (
	"net/http"
	"net/http/httptest"
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
	datasets, _ := GetDatasetDetails(client, APIServer, accessToken, datasetList, ownerGroup)
	
	// Check the result
	if len(datasets) != 0 {
		t.Errorf("Expected 0 datasets, got %d", len(datasets))
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
	datasets, _ := GetDatasetDetails(client, APIServer, accessToken, datasetList, ownerGroup)
	
	// Check the result
	if len(datasets) != 0 {
		t.Errorf("Expected 0 datasets, got %d", len(datasets))
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
	datasets, _ := GetDatasetDetails(client, APIServer, accessToken, datasetList, ownerGroup)
	
	// Check the result
	if len(datasets) != 0 {
		t.Errorf("Expected 0 datasets, got %d", len(datasets))
	}
}
