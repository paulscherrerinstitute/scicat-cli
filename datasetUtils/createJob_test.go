package datasetUtils

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestCreateJob(t *testing.T) {
	t.Run("successful job creation", func(t *testing.T) {
		// Create a mock server
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			rw.Write([]byte(`{"id": "123"}`))
		}))
		defer server.Close()
		
		// Create a client
		client := server.Client()
		
		// Define the parameters
		APIServer := server.URL
		user := map[string]string{
			"mail":        "test@example.com",
			"username":    "testuser",
			"accessToken": "testtoken",
		}
		datasetList := []string{"dataset1", "dataset2"}
		tapecopies := new(int)
		*tapecopies = 1
		
		// Call the function
		jobId := CreateJob(client, APIServer, user, datasetList, tapecopies)
		
		// Check the result
		if jobId != "123" {
			t.Errorf("Expected jobId to be '123', got '%s'", jobId)
		}
	})
	
	t.Run("server returns non-200 status code", func(t *testing.T) {
		// Create a mock server
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			rw.WriteHeader(http.StatusInternalServerError)
		}))
		defer server.Close()
		
		// Create a client
		client := server.Client()
		
		// Define the parameters
		APIServer := server.URL
		user := map[string]string{
			"mail":        "test@example.com",
			"username":    "testuser",
			"accessToken": "testtoken",
		}
		datasetList := []string{"dataset1", "dataset2"}
		tapecopies := new(int)
		*tapecopies = 1
		
		// Call the function
		jobId := CreateJob(client, APIServer, user, datasetList, tapecopies)
		
		// Check the result
		if jobId != "" {
			t.Errorf("Expected jobId to be '', got '%s'", jobId)
		}
	})
	
	t.Run("server returns invalid JSON", func(t *testing.T) {
		// Create a mock server
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			rw.Write([]byte(`invalid json`))
		}))
		defer server.Close()
		
		// Create a client
		client := server.Client()
		
		// Define the parameters
		APIServer := server.URL
		user := map[string]string{
			"mail":        "test@example.com",
			"username":    "testuser",
			"accessToken": "testtoken",
		}
		datasetList := []string{"dataset1", "dataset2"}
		tapecopies := new(int)
		*tapecopies = 1
		
		// Call the function
		jobId := CreateJob(client, APIServer, user, datasetList, tapecopies)
		
		// Check the result
		if jobId != "" {
			t.Errorf("Expected jobId to be '', got '%s'", jobId)
		}
	})
}
