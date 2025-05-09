package datasetIngestor

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestSendFilesReadyCommand(t *testing.T) {
	// Create a mock server
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		// Test method and path
		if req.URL.String() != "/Datasets/testDatasetId" {
			t.Errorf("Expected URL '/Datasets/testDatasetId', got '%s'", req.URL.String())
		}
		if req.Method != "PATCH" {
			t.Errorf("Expected method 'PATCH', got '%s'", req.Method)
		}

		// Test headers
		if req.Header.Get("Content-Type") != "application/json" {
			t.Errorf("Expected header 'Content-Type: application/json', got '%s'", req.Header.Get("Content-Type"))
		}
		if req.Header.Get("Authorization") != "Bearer testToken" {
			t.Errorf("Invalid token received: got '%s', wanted 'Bearer testToken'", req.Header.Get("Authorization"))
		}

		// Test body
		body, _ := io.ReadAll(req.Body)
		expectedBody := `{"datasetlifecycle":{"archivable":true,"archiveStatusMessage":"datasetCreated"}}`
		if strings.TrimSpace(string(body)) != expectedBody {
			t.Errorf("Expected body '%s', got '%s'", expectedBody, strings.TrimSpace(string(body)))
		}

		rw.Write([]byte(`OK`))
	}))
	// Close the server when test finishes
	defer server.Close()

	// Create a map for user info
	user := make(map[string]string)
	user["accessToken"] = "testToken"

	// Create a http client
	client := &http.Client{}

	// Call the function
	err := MarkFilesReady(client, server.URL, "testDatasetId", user)
	if err != nil {
		// TODO: write cases that trigger errors maybe
		t.Errorf("Error encountered: %v", err)
	}
}
