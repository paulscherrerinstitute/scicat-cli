package datasetUtils

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// This test checks that the function correctly parses the response from the server.
func TestGetDatasetDetailsPublished(t *testing.T) {
	// Create a mock HTTP server
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		// Respond with a JSON representation of a list of datasets
		rw.Write([]byte(`[{"pid": "1", "sourceFolder": "/folder1", "size": 100, "ownerGroup": "group1", "numberOfFiles": 10}]`))
	}))
	defer server.Close()

	// Create a new HTTP client
	client := &http.Client{}

	// Call the function with the mock server's URL and a list of dataset IDs
	datasets, urls := GetDatasetDetailsPublished(client, server.URL, []string{"1"})

	// Test that the function returns the expected results
	if len(datasets) != 1 || datasets[0].Pid != "1" {
		t.Errorf("Unexpected datasets: %v", datasets)
	}
	if len(urls) != 1 || urls[0] != "https://"+PUBLISHServer+"/datasets/folder1" {
		t.Errorf("Unexpected URLs: %v", urls)
	}
}

func TestGetDatasetDetailsPublished_MissingDatasets(t *testing.T) {
	// Create a mock HTTP server that returns a list of datasets that does not include all the requested datasets
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		rw.Write([]byte(`[{"pid": "1", "sourceFolder": "/folder1", "size": 100, "ownerGroup": "group1", "numberOfFiles": 10}]`))
	}))
	defer server.Close()

	// Create a new HTTP client
	client := &http.Client{}

	// Call the function with the mock server's URL and a list of dataset IDs
	datasets, urls := GetDatasetDetailsPublished(client, server.URL, []string{"1", "2"})

	// Since the server does not return details for all the requested datasets, the function should log a message for the missing datasets.
	// We can't directly test this with the `testing` package
	if len(datasets) != 1 || datasets[0].Pid != "1" {
		t.Errorf("Unexpected datasets: %v", datasets)
	}
	if len(urls) != 1 || urls[0] != "https://"+PUBLISHServer+"/datasets/folder1" {
		t.Errorf("Unexpected URLs: %v", urls)
	}
}

func TestGetDatasetDetailsPublished_EmptyList(t *testing.T) {
	// Create a mock HTTP server that always returns an empty list of datasets
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		rw.Write([]byte(`[]`))
	}))
	defer server.Close()

	// Create a new HTTP client
	client := &http.Client{}

	// Call the function with the mock server's URL and a list of dataset IDs
	datasets, urls := GetDatasetDetailsPublished(client, server.URL, []string{"1"})

	// Since the server returns an empty list, the function should return empty lists as well
	if len(datasets) != 0 || len(urls) != 0 {
		t.Errorf("Expected empty lists, got %v and %v", datasets, urls)
	}
}
