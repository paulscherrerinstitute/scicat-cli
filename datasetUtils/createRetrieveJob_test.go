package datasetUtils

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"encoding/json"
	"strings"
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
	jobId, _ := CreateRetrieveJob(client, APIServer, user, datasetList)
	
	// Check if the function returned a job ID
	if jobId == "" {
		t.Errorf("CreateRetrieveJob() returned an empty job ID, want non-empty")
	}
}

// checks if the function returns a valid JSON byte array and no error when it's called with valid parameters.
func TestConstructJobRequest(t *testing.T) {
	// Define the parameters for the constructJobRequest function
	user := map[string]string{
		"mail":     "test@example.com",
		"username": "testuser",
	}
	datasetList := []string{"dataset1", "dataset2"}
	
	// Call the constructJobRequest function
	bmm, err := constructJobRequest(user, datasetList)
	
	// Check if the function returned an error
	if err != nil {
		t.Errorf("constructJobRequest() returned an error: %v", err)
	}
	
	// Check if the function returned a valid JSON byte array
	var data map[string]interface{}
	if err := json.Unmarshal(bmm, &data); err != nil {
		t.Errorf("constructJobRequest() returned invalid JSON: %v", err)
	}

	// Check if the JSON byte array contains the user email and username
	if !strings.Contains(string(bmm), user["mail"]) {
		t.Errorf("constructJobRequest() did not include the user email in the request")
	}
	if !strings.Contains(string(bmm), user["username"]) {
		t.Errorf("constructJobRequest() did not include the username in the request")
	}
}

// Checks if the function returns a valid HTTP response and no error when it's called with valid parameters.
func TestSendJobRequest(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		rw.Write([]byte(`{"id": "12345"}`))
	}))
	defer server.Close()
	
	client := server.Client()
	APIServer := server.URL
	user := map[string]string{
		"mail":        "test@example.com",
		"username":    "testuser",
		"accessToken": "testtoken",
	}
	bmm := []byte(`{"key": "value"}`)
	
	resp, err := sendJobRequest(client, APIServer, user, bmm)
	
	if err != nil {
		t.Errorf("sendJobRequest() returned an error: %v", err)
	}
	
	if resp.StatusCode != 200 {
		t.Errorf("sendJobRequest() returned status code %v, want 200", resp.StatusCode)
	}
}

// Checks if the function returns a job ID and no error when it's called with a valid response.
func TestHandleJobResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		rw.Write([]byte(`{"id": "12345"}`))
	}))
	defer server.Close()
	
	client := server.Client()
	resp, _ := client.Get(server.URL)
	user := map[string]string{
		"mail":     "test@example.com",
		"username": "testuser",
	}
	
	jobId, err := handleJobResponse(resp, user)
	
	if err != nil {
		t.Errorf("handleJobResponse() returned an error: %v", err)
	}
	
	if jobId != "12345" {
		t.Errorf("handleJobResponse() returned job ID %v, want 12345", jobId)
	}
}
