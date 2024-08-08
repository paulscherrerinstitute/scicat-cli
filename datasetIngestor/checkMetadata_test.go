package datasetIngestor

import (
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"
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
	var TEST_API_SERVER string = "https://dacat-qa.psi.ch/api/v3" // TODO: Test Improvement. Change this to a mock server. At the moment, tests will fail if we change this to a mock server.
	var APIServer = TEST_API_SERVER
	var metadatafile1 = "testdata/metadata.json"
	var metadatafile2 = "testdata/metadata-short.json"

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
		"displayName": "csaxsswissfel",
		"mail":        "testuser@example.com",
	}

	// Mock access groups
	accessGroups := []string{"group1", "group2"}

	// Call the function with mock parameters
	metaDataMap, sourceFolder, beamlineAccount, err := ReadAndCheckMetadata(client, APIServer, metadatafile1, user, accessGroups)
	if err != nil {
		t.Error("Error in CheckMetadata function: ", err)
	}

	// Add assertions here based on the expected behavior of the function
	if len(metaDataMap) == 0 {
		t.Error("Expected non-empty metadata map")
	}
	if sourceFolder == "" {
		t.Error("Expected non-empty source folder")
	}
	if sourceFolder != "/usr/share/gnome" {
		t.Error("sourceFolder should be '/usr/share/gnome'")
	}
	if reflect.TypeOf(beamlineAccount).Kind() != reflect.Bool {
		t.Error("Expected beamlineAccount to be boolean")
	}
	if beamlineAccount != false {
		t.Error("Expected beamlineAccount to be false")
	}
	if _, ok := metaDataMap["ownerEmail"]; !ok {
		t.Error("metaDataMap missing required key 'ownerEmail'")
	}
	if _, ok := metaDataMap["principalInvestigator"]; !ok {
		t.Error("metaDataMap missing required key 'principalInvestigator'")
	}
	if _, ok := metaDataMap["scientificMetadata"]; !ok {
		t.Error("metaDataMap missing required key 'scientificMetadata'")
	}
	scientificMetadata, ok := metaDataMap["scientificMetadata"].([]interface{})
	if ok {
		firstEntry := scientificMetadata[0].(map[string]interface{})
		sample, ok := firstEntry["sample"].(map[string]interface{})
		if ok {
			if _, ok := sample["name"]; !ok {
				t.Error("Sample is missing 'name' field")
			}
			if _, ok := sample["description"]; !ok {
				t.Error("Sample is missing 'description' field")
			}
		}
	} else {
		t.Error("scientificMetadata is not a list")
	}

	// test with the second metadata file
	metaDataMap2, sourceFolder2, beamlineAccount2, err := ReadAndCheckMetadata(client, APIServer, metadatafile2, user, accessGroups)
	if err != nil {
		t.Error("Error in CheckMetadata function: ", err)
	}

	// Add assertions here based on the expected behavior of the function
	if len(metaDataMap2) == 0 {
		t.Error("Expected non-empty metadata map")
	}
	if sourceFolder2 == "" {
		t.Error("Expected non-empty source folder")
	}
	if sourceFolder2 != "/tmp/gnome" {
		t.Error("sourceFolder should be '/tmp/gnome'")
	}
	if reflect.TypeOf(beamlineAccount2).Kind() != reflect.Bool {
		t.Error("Expected beamlineAccount to be boolean")
	}
	if beamlineAccount2 != false {
		t.Error("Expected beamlineAccount to be false")
	}
}

func TestCheckMetadata_CrashCase(t *testing.T) {
	// Define mock parameters for the function
	var TEST_API_SERVER string = "https://dacat-qa.psi.ch/api/v3" // TODO: Test Improvement. Change this to a mock server. At the moment, tests will fail if we change this to a mock server.
	var APIServer = TEST_API_SERVER
	var metadatafile3 = "testdata/metadata_illegal.json"

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
		"displayName": "csaxsswissfel",
		"mail":        "testuser@example.com",
	}

	// Mock access groups
	accessGroups := []string{"group1", "group2"}

	// Call the function that should return an error
	_, _, _, err := ReadAndCheckMetadata(client, APIServer, metadatafile3, user, accessGroups)

	// Check that the function returned the expected error
	if err == nil {
		t.Fatal("Function did not return an error as expected")
	} else if !strings.Contains(err.Error(), ErrIllegalKeys) {
		t.Errorf("Expected error to contain%q, got %q", ErrIllegalKeys, err.Error())
	} else if !strings.Contains(err.Error(), "description.") || !strings.Contains(err.Error(), "name]") {
		t.Errorf("Expected error to list the following illegal keys: \"description.\", \"name]\"")
	}
}
