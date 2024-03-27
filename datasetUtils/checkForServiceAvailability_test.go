package datasetUtils

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"bytes"
	"gopkg.in/yaml.v2"
)

func TestReadYAMLFile(t *testing.T) {
	// Create a mock HTTP server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/repos/paulscherrerinstitute/scicat-cli/releases/latest":
			// Respond with a mock release
			fmt.Fprintln(w, `{"tag_name": "v1.0.0"}`)
		case "/paulscherrerinstitute/scicat-cli/releases/download/v1.0.0/cmd/datasetIngestor/datasetIngestorServiceAvailability.yml":
			// Respond with a mock YAML file
			fmt.Fprintln(w, "mock: YAML file")
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	
	// Update the GitHubAPI and GitHubMainLocation variables to point to the mock server
	oldGitHubAPI := GitHubAPI
	oldGitHubMainLocation := GitHubMainLocation
	GitHubAPI = server.URL + "/repos/paulscherrerinstitute/scicat-cli/releases/latest"
	GitHubMainLocation = server.URL + "/paulscherrerinstitute/scicat-cli/releases/download/v1.0.0"
	defer func() {
		// Restore the original variables after the test
		GitHubAPI = oldGitHubAPI
		GitHubMainLocation = oldGitHubMainLocation
	}()
		
	// Create a new HTTP client
	client := &http.Client{}
	
	// Call the function
	yamlFile, err := readYAMLFile(client)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	
	// Check that the function returned the expected YAML file
	expected := []byte("mock: YAML file\n")
	if !bytes.Equal(yamlFile, expected) {
		t.Errorf("Expected %q, got %q", expected, yamlFile)
	}
}

func TestReadYAMLFileIntegration(t *testing.T) {
	// Create a new HTTP client
	client := &http.Client{}
	
	// Call the function
	yamlFile, err := readYAMLFile(client)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	
	// Check that the function returned a non-empty file
	if len(yamlFile) == 0 {
		t.Errorf("Expected a non-empty YAML file, got an empty file")
	}
}

func TestYAMLStructure(t *testing.T) {
	// The test will fail if the indentation of yamlFile is not correct
	yamlFile := []byte(`
production:
  ingest: 
    status: on
  archive:
    status: on
qa:
  ingest: 
    status: on
  archive:
    status: on
`)

	var serviceAvailability ServiceAvailability

	err := yaml.Unmarshal(yamlFile, &serviceAvailability)
	if err != nil {
			t.Fatalf("Expected no error, got %v", err)
	}

	checkService := func(service Availability, serviceName string) {
		if service.Status != "on" && service.Status != "down" {
			t.Errorf("Expected status 'on' or 'down' for %s, got %q", serviceName, service.Status)
		}

		if service.Status == "down" {
			if service.Downfrom == "" {
				t.Errorf("Expected 'downfrom' for %s when status is 'down'", serviceName)
			}

			if service.Downto == "" {
				t.Errorf("Expected 'downto' for %s when status is 'down'", serviceName)
			}

			if service.Comment == "" {
				t.Errorf("Expected 'comment' for %s when status is 'down'", serviceName)
			}
		}
	}

	checkService(serviceAvailability.Production.Ingest, "production ingest")
	checkService(serviceAvailability.Production.Archive, "production archive")
	checkService(serviceAvailability.Qa.Ingest, "qa ingest")
	checkService(serviceAvailability.Qa.Archive, "qa archive")
}
