package datasetUtils

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"bytes"
	"log"
)

func TestFetchLatestVersion(t *testing.T) {
	// Create a mock HTTP server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Return a JSON response similar to the GitHub API
		w.Write([]byte(`{"tag_name": "v1.0.0"}`))
	}))
	defer server.Close()
	
	// Replace GitHubAPI with the URL of the mock server
	oldGitHubAPI := GitHubAPI
	GitHubAPI = server.URL
	defer func() { GitHubAPI = oldGitHubAPI }()
	
	// Create a mock HTTP client
	client := server.Client()
	
	// Call fetchLatestVersion
	version, err := fetchLatestVersion(client)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	
	// Check the version number
	if version != "v1.0.0" {
		t.Errorf("Expected version v1.0.0, got %s", version)
	}
}

type MockUserInput struct {
    Input string
}

func (m MockUserInput) ReadLine() (string, error) {
    return m.Input, nil
}

func TestCheckForNewVersion(t *testing.T) {
	tests := []struct {
		name           string
		currentVersion string
		mockResponse   string
		expectedLog    string
		}{
			{
				name:           "New version available, non-interactive mode",
				currentVersion: "0.9.0",
				mockResponse:   `{"tag_name": "v1.0.0"}`,
				expectedLog:    "You can find the download instructions here: https://github.com/paulscherrerinstitute/scicat-cli?tab=readme-ov-file#manual-deployment",
			},
			{
				name:           "No new version available, non-interactive mode",
				currentVersion: "1.0.0",
				mockResponse:   `{"tag_name": "v1.0.0"}`,
				expectedLog:    "Your version of this program is up-to-date",
			},
		}
		
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a mock HTTP server
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				// Return a JSON response similar to the GitHub API
				w.Write([]byte(tt.mockResponse))
			}))
			defer server.Close()
			
			// Replace GitHubAPI with the URL of the mock server
			oldGitHubAPI := GitHubAPI
			GitHubAPI = server.URL
			defer func() { GitHubAPI = oldGitHubAPI }()
			
			// Create a mock HTTP client
			client := server.Client()
			
			// Call CheckForNewVersion
			CheckForNewVersion(client, "test", tt.currentVersion)
			
			// Check the log output
			logOutput := getLogOutput()
			if !strings.Contains(logOutput, tt.expectedLog) {
				t.Errorf("Expected log message not found: %s", logOutput)
			}
			
			// Clear the log buffer after each test
			buf.Reset()
		})
	}
}
	
var buf bytes.Buffer

func init() {
	// Redirect the output of the logger to buf
	log.SetOutput(&buf)
}

func getLogOutput() string {
	return buf.String()
}
