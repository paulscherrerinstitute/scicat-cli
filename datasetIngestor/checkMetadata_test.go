package datasetIngestor

import (
	"net/http"
	"testing"
	"time"
	"reflect"
)

func TestGetHost(t *testing.T) {
    // Call the function under test.
		host := GetHost()

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
	// Define mock parameters for the function
	var TEST_API_SERVER string = "https://dacat-qa.psi.ch/api/v3" // "https://example.com/api"
	var APIServer = TEST_API_SERVER
	var metadatafile1 = "testdata/metadata.json"
	// var metadatafile2 = "testdata/metadata-short.json"

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
	}

	// Mock access groups
	accessGroups := []string{"group1", "p17301"}

	// Call the function with mock parameters
	metaDataMap, sourceFolder, beamlineAccount := CheckMetadata(client, APIServer, metadatafile1, user, accessGroups)

	// Add assertions here based on the expected behavior of the function
	if len(metaDataMap) == 0 {
		t.Error("Expected non-empty metadata map")
	}
	if sourceFolder == "" {
		t.Error("Expected non-empty source folder")
	}
	if reflect.TypeOf(beamlineAccount).Kind() != reflect.Bool {
		t.Error("Expected beamlineAccount to be boolean")
	}
}
