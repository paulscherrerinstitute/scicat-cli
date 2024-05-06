package datasetUtils

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// Checks if the function returns a job ID when it successfully creates a job.
func TestCreateRetrieveJob(t *testing.T) {
	// Create a test server that always responds with a 200 status code and a job ID
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		rw.Write([]byte(`{"id": "12345"}`))
	}))
	defer server.Close()
	
	// Create a test client that uses the test server
	client := server.Client()
	
	// Define the parameters for the CreateRetrieveJob function
	APIServer := server.URL
	user := map[string]string{
		"mail":        "test@example.com",
		"username":    "testuser",
		"accessToken": "testtoken",
	}
	datasetList := []string{"dataset1", "dataset2"}
	
	// Call the CreateRetrieveJob function
	jobId := CreateRetrieveJob(client, APIServer, user, datasetList)
	
	// Check if the function returned a job ID
	if jobId == "" {
		t.Errorf("CreateRetrieveJob() returned an empty job ID, want non-empty")
	}
}
