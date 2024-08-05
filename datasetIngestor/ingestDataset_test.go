package datasetIngestor

import (
	"bytes"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
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
		Timeout:   5 * time.Second, // Set a timeout for requests
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
		"displayName": "test user",
	}

	// Mock metaDataMap
	metaDataMap := map[string]interface{}{
		"type": "raw",
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
	datasetId, err := IngestDataset(client, server.URL, metaDataMap, datafiles, user)
	if err != nil {
		t.Errorf("received unexpected error: %v", err)
	}
	if datasetId != "test-dataset-id" {
		t.Errorf("Expected dataset id 'test-dataset-id', but got '%s'", datasetId)
	}
}

func TestGetEndpoint(t *testing.T) {
	// Redirect log output to a buffer
	var buf bytes.Buffer
	log.SetOutput(&buf)
	defer func() {
		log.SetOutput(os.Stderr)
	}()

	testCases := []struct {
		dsType string
		want   string
	}{
		{"raw", "/RawDatasets"},
		{"derived", "/DerivedDatasets"},
		{"base", "/Datasets"},
		{"unknown", ""},
	}

	for _, tc := range testCases {
		got, err := getEndpoint(tc.dsType)
		if err != nil && tc.dsType != "unknown" {
			t.Errorf("getEndpoint(%q) returned unexpected error: %v", tc.dsType, err)
		}
		if got != tc.want {
			t.Errorf("getEndpoint(%q) = %q; want %q", tc.dsType, got, tc.want)
		}
		if tc.dsType == "unknown" && err == nil {
			t.Errorf("Expected error for unknown dataset type not found")
		}
		buf.Reset()
	}
}

func TestSendRequest(t *testing.T) {
	// Create a test server
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	// Create a test client
	client := &http.Client{}

	// Call the sendRequest function
	resp, err := sendRequest(client, "GET", ts.URL, nil)
	if err != nil {
		t.Errorf("received unexpected error: %v", err)
	}

	// Check the response
	if resp.StatusCode != http.StatusOK {
		t.Errorf("sendRequest() returned status %d; want %d", resp.StatusCode, http.StatusOK)
	}
}

func TestDecodePid(t *testing.T) {
	// Create a test response
	resp := &http.Response{
		Body: io.NopCloser(strings.NewReader(`{"pid": "12345"}`)),
	}

	// Call the decodePid function
	pid, err := decodePid(resp)
	if err != nil {
		t.Errorf("received unexpected error: %v", err)
	}

	// Check the returned pid
	if pid != "12345" {
		t.Errorf("decodePid() returned pid %q; want %q", pid, "12345")
	}
}

func TestCreateOrigDatablocks(t *testing.T) {
	// Define test cases
	testCases := []struct {
		name             string
		datafiles        []Datafile
		expectedRequests int
	}{
		{
			name:             "Case 1: BLOCK_MAXFILES > len(datafiles)",
			datafiles:        makeDatafiles(10000, BLOCK_MAXBYTES/10000),
			expectedRequests: 1,
		},
		{
			name:             "Case 2: BLOCK_MAXFILES < len(datafiles)",
			datafiles:        makeDatafiles(40000, BLOCK_MAXBYTES/10000),
			expectedRequests: 4,
		},
		{
			name:             "Case 3: BLOCK_MAXFILES = len(datafiles)",
			datafiles:        makeDatafiles(20000, BLOCK_MAXBYTES/20000),
			expectedRequests: 1,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Keep track of the number of requests
			var numRequests int

			// Create a mock HTTP server
			server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
				// Increment the request count for each request
				numRequests++

				// Check if the request method is POST
				if req.Method != http.MethodPost {
					t.Errorf("Expected POST request, got %s", req.Method)
				}

				// Check if the request URL is correct
				expectedURL := "/OrigDatablocks?access_token=testToken"
				if req.URL.String() != expectedURL {
					t.Errorf("Expected request to %s, got %s", expectedURL, req.URL.String())
				}

				rw.WriteHeader(http.StatusOK)
			}))
			// Close the server when test finishes
			defer server.Close()

			// Create a mock HTTP client
			client := server.Client()

			// Define user data
			user := map[string]string{
				"accessToken": "testToken",
			}

			// Call the function with test data
			createOrigDatablocks(client, server.URL, tc.datafiles, "testDatasetId", user)

			// Check if the correct number of requests were made
			if numRequests != tc.expectedRequests {
				t.Errorf("Expected %d requests, got %d", tc.expectedRequests, numRequests)
			}
		})
	}
}

func makeDatafiles(numFiles, size int) []Datafile {
	datafiles := make([]Datafile, numFiles)
	for i := range datafiles {
		datafiles[i] = Datafile{Size: int64(size)}
	}
	return datafiles
}
