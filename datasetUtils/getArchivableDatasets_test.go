package datasetUtils

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// This suite includes two test cases: one where `ownerGroup` is provided and one where `inputdatasetList` is provided.
func TestGetArchivableDatasets(t *testing.T) {
	tests := []struct {
		name             string
		serverResponse   string
		ownerGroup       string
		inputdatasetList []string
		expected         []string
	}{
		{
			name:             "Test with ownerGroup",
			serverResponse:   `[{"pid":"1","sourceFolder":"folder1","size":10},{"pid":"2","sourceFolder":"folder2","size":0},{"pid":"3","sourceFolder":"folder3","size":20}]`,
			ownerGroup:       "testGroup",
			inputdatasetList: []string{},
			expected:         []string{"1", "3"},
		},
		{
			name:             "Test without ownerGroup",
			serverResponse:   `[{"pid":"1","sourceFolder":"folder1","size":10},{"pid":"2","sourceFolder":"folder2","size":0},{"pid":"3","sourceFolder":"folder3","size":20}]`,
			ownerGroup:       "",
			inputdatasetList: []string{"1", "2", "3"},
			expected:         []string{"1", "3"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a mock HTTP server
			server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
				// Send response to be tested
				rw.Write([]byte(tt.serverResponse))
			}))
			// Close the server when test finishes
			defer server.Close()

			// Use Client & URL from our local test server
			client := server.Client()
			APIServer := server.URL
			accessToken := "testToken"

			// Call our function
			datasetList, err := GetArchivableDatasets(client, APIServer, tt.ownerGroup, tt.inputdatasetList, accessToken)
			if err != nil {
				t.Errorf("Unexpected error: %s", err.Error())
			}

			// Check if the function results match our expectations
			if len(datasetList) != len(tt.expected) {
				t.Errorf("Expected length %v but got %v", len(tt.expected), len(datasetList))
			}

			for i, v := range datasetList {
				if v != tt.expected[i] {
					t.Errorf("Expected %v but got %v", tt.expected[i], v)
				}
			}
		})
	}
}

// This test suite includes two test cases: one where the server response includes datasets that are archivable (size > 0), and one where none of the datasets are archivable.
func TestAddResult(t *testing.T) {
	tests := []struct {
		name           string
		serverResponse string
		filter         string
		datasetList    []string
		expected       []string
	}{
		{
			name:           "Test with archivable datasets",
			serverResponse: `[{"pid":"1","sourceFolder":"folder1","size":10},{"pid":"2","sourceFolder":"folder2","size":0},{"pid":"3","sourceFolder":"folder3","size":20}]`,
			filter:         "size>0",
			datasetList:    []string{},
			expected:       []string{"1", "3"},
		},
		{
			name:           "Test without archivable datasets",
			serverResponse: `[{"pid":"1","sourceFolder":"folder1","size":0},{"pid":"2","sourceFolder":"folder2","size":0},{"pid":"3","sourceFolder":"folder3","size":0}]`,
			filter:         "size>0",
			datasetList:    []string{},
			expected:       []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a mock HTTP server
			server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
				// Send response to be tested
				rw.Write([]byte(tt.serverResponse))
			}))
			// Close the server when test finishes
			defer server.Close()

			// Use Client & URL from our local test server
			client := server.Client()
			APIServer := server.URL
			accessToken := "testToken"

			// Call our function
			datasetList, err := addResult(client, APIServer, tt.filter, accessToken, tt.datasetList)
			if err != nil {
				t.Errorf("Error: %v", err)
			}

			// Check if the function results match our expectations
			if len(datasetList) != len(tt.expected) {
				t.Errorf("Expected length %v but got %v", len(tt.expected), len(datasetList))
			}

			for i, v := range datasetList {
				if v != tt.expected[i] {
					t.Errorf("Expected %v but got %v", tt.expected[i], v)
				}
			}
		})
	}
}
