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

func TestGenerateDownloadURL(t *testing.T) {
	deployLocation := "https://github.com/paulscherrerinstitute/scicat-cli/releases/download"
	latestVersion := "0.1.0"
	
	testCases := []struct {
		osName      string
		expectedURL string
		}{
			{
				osName:      "Linux",
				expectedURL: "https://github.com/paulscherrerinstitute/scicat-cli/releases/download/v0.1.0/scicat-cli_.0.1.0_Linux_x86_64.tar.gz",
			},
			{
				osName:      "Windows",
				expectedURL: "https://github.com/paulscherrerinstitute/scicat-cli/releases/download/v0.1.0/scicat-cli_.0.1.0_Windows_x86_64.zip",
			},
			{
				osName:      "Darwin",
				expectedURL: "https://github.com/paulscherrerinstitute/scicat-cli/releases/download/v0.1.0/scicat-cli_.0.1.0_Darwin_x86_64.tar.gz",
			},
		}
		
	for _, testCase := range testCases {
		actualURL := generateDownloadURL(deployLocation, latestVersion, testCase.osName)
		
		if actualURL != testCase.expectedURL {
			t.Errorf("Expected URL to be %s, but got %s", testCase.expectedURL, actualURL)
		}
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
		interactiveFlag bool
		userInput string
		}{
			{
				name:           "New version available, non-interactive mode",
				currentVersion: "0.9.0",
				mockResponse:   `{"tag_name": "v1.0.0"}`,
				expectedLog:    "You should upgrade to a newer version",
				interactiveFlag: false,
				userInput: "y\n",
			},
			{
				name:           "No new version available, non-interactive mode",
				currentVersion: "1.0.0",
				mockResponse:   `{"tag_name": "v1.0.0"}`,
				expectedLog:    "Your version of this program is up-to-date",
				interactiveFlag: false,
				userInput: "y\n",
			},
			{
				name:           "New version available, interactive mode",
				currentVersion: "0.9.0",
				mockResponse:   `{"tag_name": "v1.0.0"}`,
				expectedLog:    "You should upgrade to a newer version",
				interactiveFlag: true,
				userInput: "y\n",
			},
			{
				name:           "New version available, interactive mode, no upgrade",
				currentVersion: "0.9.0",
				mockResponse:   `{"tag_name": "v1.0.0"}`,
				expectedLog:    "Warning: Execution stopped, please update the program now.",
				interactiveFlag: true,
				userInput: "n\n",
			},
			{
				name:           "New path available, interactive mode",
				currentVersion: "0.9.0",
				mockResponse:   `{"tag_name": "v0.9.1"}`,
				expectedLog:    "You should upgrade to a newer version",
				interactiveFlag: true,
				userInput: "y\n",
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
			CheckForNewVersion(client, "test", tt.currentVersion, tt.interactiveFlag, MockUserInput{Input: tt.userInput})
			
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
