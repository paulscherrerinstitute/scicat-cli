package datasetIngestor

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
)

func TestGetHost(t *testing.T) {
	// Call the function under test.
	host := GetHost()

	// fail the test and report an error if the returned hostname is an empty string.
	if len(host) == 0 {
		t.Errorf("getHost() returned an empty string")
	}

	// OUTDATED: getHost will return unknown if we can't get a hostname that is FQDN
	// fail the test and report an error if the returned hostname is "unknown".
	/*if host == "unknown" {
		t.Errorf("getHost() was unable to get the hostname")
	}*/

	//TODO: write better test for this if necessary
}

func TestCheckMetadata(t *testing.T) {
	// Define mock parameters for the function
	var metadatafile1 = "testdata/metadata.json"
	var metadatafile2 = "testdata/metadata-short.json"

	var numRequests uint = 0
	// Create a mock HTTP server
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		// Increment the request count for each request
		numRequests++

		// Check if the request method is POST
		if req.Method != http.MethodPost {
			t.Errorf("Expected POST request, got %s", req.Method)
		}

		// Check if the request URL is correct
		expectedURL := "/datasets/isValid"
		if req.URL.String() != expectedURL {
			t.Errorf("Expected request to %s, got %s", expectedURL, req.URL.String())
		}

		rw.WriteHeader(http.StatusOK)
		rw.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(rw, `{"valid":true}`)
	}))
	// Close the server when test finishes
	defer server.Close()

	// Mock user map
	user := map[string]string{
		"displayName": "csaxsswissfel",
		"mail":        "testuser@example.com",
	}

	// Mock access groups
	accessGroups := []string{"group1", "group2"}

	// Call the function with mock parameters
	metaDataMap, sourceFolder, beamlineAccount, err := ReadAndCheckMetadata(server.Client(), server.URL, metadatafile1, user, accessGroups)
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
	metaDataMap2, sourceFolder2, beamlineAccount2, err := ReadAndCheckMetadata(server.Client(), server.URL, metadatafile2, user, accessGroups)
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
	var metadatafile3 = "testdata/metadata_illegal.json"

	var numRequests uint = 0
	// Create a mock HTTP server
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		// Increment the request count for each request
		numRequests++

		// Check if the request method is POST
		if req.Method != http.MethodPost {
			t.Errorf("Expected POST request, got %s", req.Method)
		}

		// Check if the request URL is correct
		expectedURL := "/datasets/isValid"
		if req.URL.String() != expectedURL {
			t.Errorf("Expected request to %s, got %s", expectedURL, req.URL.String())
		}

		rw.WriteHeader(http.StatusOK)
		rw.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(rw, `{"valid":false}`)
	}))
	// Close the server when test finishes
	defer server.Close()

	// Create a mock HTTP client
	client := server.Client()

	// Mock user map
	user := map[string]string{
		"displayName": "csaxsswissfel",
		"mail":        "testuser@example.com",
	}

	// Mock access groups
	accessGroups := []string{"group1", "group2"}

	// Call the function that should return an error
	_, _, _, err := ReadAndCheckMetadata(client, server.URL, metadatafile3, user, accessGroups)

	// Check that the function returned the expected error
	if err == nil {
		t.Fatal("Function did not return an error as expected")
	} else if !strings.Contains(err.Error(), ErrIllegalKeys) {
		t.Errorf("Expected error to contain%q, got %q", ErrIllegalKeys, err.Error())
	} else if !strings.Contains(err.Error(), "description.") || !strings.Contains(err.Error(), "name]") {
		t.Errorf("Expected error to list the following illegal keys: \"description.\", \"name]\"")
	}
}

func TestCheckMetadata_ValidFalse(t *testing.T) {
	var metadatafile2 = "testdata/metadata-short.json"
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPost {
			t.Errorf("Expected POST request, got %s", req.Method)
		}
		expectedURL := "/datasets/isValid"
		if req.URL.String() != expectedURL {
			t.Errorf("Expected request to %s, got %s", expectedURL, req.URL.String())
		}

		rw.WriteHeader(http.StatusOK)
		rw.Header().Set("Content-Type", "application/json")
		// metadata-short.json is a valid JSON file, but the response is mocked here to check
		// that the error message from the response is correctly propagated in the error returned by the function
		fmt.Fprintf(rw, `{"valid":false, "error": "sourceFolderHost must be a URL address"}`)
	}))
	// Close the server when test finishes
	defer server.Close()
	client := server.Client()

	// Mock user map
	user := map[string]string{
		"displayName": "csaxsswissfel",
		"mail":        "testuser@example.com",
	}
	accessGroups := []string{"group1", "group2"}

	// Call the function with mock parameters
	_, _, _, err := ReadAndCheckMetadata(client, server.URL, metadatafile2, user, accessGroups)
	if err == nil {
		t.Fatal("Function did not return an error as expected")
	} else if !strings.Contains(err.Error(), "metadata is not valid") {
		t.Errorf("Expected error to contain 'metadata is not valid', got %q", err.Error())
	} else if !strings.Contains(err.Error(), "sourceFolderHost must be a URL address") {
		t.Errorf("Expected error to contain 'sourceFolderHost must be a URL address', got %q", err.Error())
	}
}

func TestCheckMetadata_BeamlineAccount(t *testing.T) {
	var metadatafile2 = "testdata/metadata-short.json"
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPost {
			t.Errorf("Expected POST request, got %s", req.Method)
		}
		expectedURL := "/datasets/isValid"
		if req.URL.String() != expectedURL {
			t.Errorf("Expected request to %s, got %s", expectedURL, req.URL.String())
		}

		rw.WriteHeader(http.StatusOK)
		rw.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(rw, `{"valid":true}`)
	}))
	// Close the server when test finishes
	defer server.Close()
	client := server.Client()

	// Mock user map
	user := map[string]string{
		"displayName": "slscsaxs1",
		"mail":        "testuser@example.com",
	}
	accessGroups := []string{"slscsaxs", "slscsaxs1"}

	// Call the function with mock parameters
	_, _, beamlineAccount, err := ReadAndCheckMetadata(client, server.URL, metadatafile2, user, accessGroups)
	if err != nil {
		t.Error("Error in CheckMetadata function: ", err)
	}
	if !beamlineAccount {
		t.Error("Expected beamline account to be true")
	}
}
