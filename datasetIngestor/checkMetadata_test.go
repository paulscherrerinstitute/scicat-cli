package datasetIngestor

import (
	"encoding/json"
	"io/ioutil"
	"io"
	"net/http"
	"strings"
	"testing"
	"os"
)

// TestGetHost is a test function for the getHost function.
func TestGetHost(t *testing.T) {
    // Call the function under test.
    host := getHost()

    // fail the test and report an error if the returned hostname is an empty string.
    if len(host) == 0 {
        t.Errorf("getHost() returned an empty string")
    }

    // fail the test and report an error if the returned hostname is "unknown".
    if host == "unknown" {
        t.Errorf("getHost() was unable to get the hostname")
    }
}

func TestCheckMetadata(t *testing.T) {
	// Mock HTTP client
	mockClient := &http.Client{
		Transport: RoundTripFunc(func(req *http.Request) *http.Response {
			// Prepare a mock response for the HTTP client
			return &http.Response{
				StatusCode: 200,
				Body:       io.NopCloser(strings.NewReader(`{"valid":true}`)),
				Header:     make(http.Header),
			}
		}),
	}

	// Mock user map
	mockUser := map[string]string{
		"displayName": "testuser",
		"mail":        "testuser@example.com",
	}

	// Mock access groups
	mockAccessGroups := []string{"group1", "group2"}

	// Mock metadata file content
	mockMetadata := map[string]interface{}{
		"type": "raw",
		// Add other required fields as needed for testing
	}

	// Convert metadata to JSON
	mockMetadataJSON, err := json.Marshal(mockMetadata)
	if err != nil {
		t.Fatalf("Error marshaling mock metadata: %v", err)
	}

	// Create a temporary file for mock metadata
	tmpfile, err := ioutil.TempFile("", "mockmetadata.json")
	if err != nil {
		t.Fatalf("Error creating temporary file: %v", err)
	}
	defer tmpfile.Close()
	defer func() {
		// Clean up temporary file
		if err := tmpfile.Close(); err != nil {
			t.Fatalf("Error closing temporary file: %v", err)
		}
		if err := os.Remove(tmpfile.Name()); err != nil {
			t.Fatalf("Error removing temporary file: %v", err)
		}
	}()

	// Write mock metadata JSON to the temporary file
	if _, err := tmpfile.Write(mockMetadataJSON); err != nil {
		t.Fatalf("Error writing mock metadata to temporary file: %v", err)
	}

	// Call the function with mock parameters
	metaDataMap, sourceFolder, beamlineAccount := CheckMetadata(mockClient, "http://example.com/api", tmpfile.Name(), mockUser, mockAccessGroups)

	// Add assertions here based on the expected behavior of the function
	// For example:
	if len(metaDataMap) == 0 {
		t.Error("Expected non-empty metadata map")
	}
	if sourceFolder == "" {
		t.Error("Expected non-empty source folder")
	}
	if !beamlineAccount {
		t.Error("Expected beamline account to be true")
	}
}

// RoundTripFunc type is a custom implementation of http.RoundTripper
type RoundTripFunc func(req *http.Request) *http.Response

// RoundTrip executes a single HTTP transaction, returning a Response for the provided Request.
func (f RoundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req), nil
}
