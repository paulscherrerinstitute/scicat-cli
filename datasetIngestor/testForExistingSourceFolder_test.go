package datasetIngestor

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTestForExistingSourceFolder(t *testing.T) {
	t.Run("test with empty response", func(t *testing.T) {
		// Create a mock server
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			// Test request parameters
			assert.Equal(t, req.URL.String(), "/Datasets?access_token=testToken")
			// Send response to be tested
			rw.Write([]byte(`[]`))
		}))
		// Close the server when test finishes
		defer server.Close()

		// Use Client & URL from our local test server
		client := server.Client()
		APIServer := server.URL
		accessToken := "testToken"

		folders := []string{"folder1", "folder2"}

		// TODO test the results of this function
		TestForExistingSourceFolder(folders, client, APIServer, accessToken)
	})

	t.Run("test with existing folders and allowExistingSourceFolder true", func(t *testing.T) {
		// Create a mock server
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			// Test request parameters
			assert.Equal(t, req.URL.String(), "/Datasets?access_token=testToken")
			// Send response to be tested
			rw.Write([]byte(`[{"folder": "folder1"}]`))
		}))
		// Close the server when test finishes
		defer server.Close()

		// Use Client & URL from our local test server
		client := server.Client()
		APIServer := server.URL
		accessToken := "testToken"

		folders := []string{"folder1", "folder2"}

		// TODO test the results of this function.
		TestForExistingSourceFolder(folders, client, APIServer, accessToken)
	})
}

func TestProcessResponse(t *testing.T) {
	// Test with valid JSON
	validJSON := `[{"pid": "123", "sourceFolder": "folder", "size": 100}]`
	resp := &http.Response{
		Body: io.NopCloser(bytes.NewBufferString(validJSON)),
	}
	result, err := processResponse(resp)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if len(result) != 1 || result[0].Pid != "123" || result[0].SourceFolder != "folder" || result[0].Size != 100 {
		t.Errorf("Unexpected result: %v", result)
	}

	// Test with invalid JSON
	invalidJSON := `{"pid": "123", "sourceFolder": "folder", "size": 100}`
	resp = &http.Response{
		Body: io.NopCloser(bytes.NewBufferString(invalidJSON)),
	}
	result, err = processResponse(resp)
	if err == nil {
		t.Errorf("Expected error from processResponse")
	}
	if len(result) != 0 {
		t.Errorf("Expected empty QueryResult, got '%v'", result)
	}

	// Test with empty body
	resp = &http.Response{
		Body: io.NopCloser(bytes.NewBufferString("")),
	}
	result, err = processResponse(resp)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if len(result) != 0 {
		t.Errorf("Expected empty QueryResult, got '%v'", result)
	}
}
