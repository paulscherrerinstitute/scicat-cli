package datasetIngestor

import (
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
		allowExistingSourceFolder := false
		
		folders := []string{"folder1", "folder2"}
		
		TestForExistingSourceFolder(folders, client, APIServer, accessToken, &allowExistingSourceFolder)
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
		allowExistingSourceFolder := true
		
		folders := []string{"folder1", "folder2"}
		
		TestForExistingSourceFolder(folders, client, APIServer, accessToken, &allowExistingSourceFolder)
	})
}
