package datasetUtils

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
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

	// Remove the creationTime field from the actual JSON
	delete(data, "creationTime")

	// Define the expected data
	expectedData := map[string]interface{}{
		"emailJobInitiator": user["mail"],
		"jobParams": map[string]interface{}{
			"username":        user["username"],
			"destinationPath": "/archive/retrieve",
		},
		"datasetList": []interface{}{
			map[string]interface{}{"pid": datasetList[0], "files": []interface{}{}},
			map[string]interface{}{"pid": datasetList[1], "files": []interface{}{}},
		},
		"jobStatusMessage": "jobSubmitted",
		"type":             "retrieve",
	}

	// Compare individual fields
	for key, expectedValue := range expectedData {
		if actualValue, ok := data[key]; ok {
			if key == "datasetList" {
				// Assert the underlying type of actualValue and expectedValue to []interface{}
				actualList, ok1 := actualValue.([]interface{})
				expectedList, ok2 := expectedValue.([]interface{})
				if !ok1 || !ok2 {
					t.Errorf("constructJobRequest() returned unexpected type for key %v: got %T want %T", key, actualValue, expectedValue)
					continue
				}

				// Sort the datasetList slice before comparing
				sort.Slice(actualList, func(i, j int) bool {
					return actualList[i].(map[string]interface{})["pid"].(string) < actualList[j].(map[string]interface{})["pid"].(string)
				})
				sort.Slice(expectedList, func(i, j int) bool {
					return expectedList[i].(map[string]interface{})["pid"].(string) < expectedList[j].(map[string]interface{})["pid"].(string)
				})

				actualValue = actualList
				expectedValue = expectedList
			}
			if !reflect.DeepEqual(actualValue, expectedValue) {
				t.Errorf("constructJobRequest() returned unexpected JSON for key %v: got %v want %v", key, actualValue, expectedValue)
			}
		} else {
			t.Errorf("constructJobRequest() did not return expected key in JSON: %v", key)
		}
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
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Errorf("sendJobRequest() returned status code %v, want 200", resp.StatusCode)
	}
}

// Checks for a successful response, a response with a non-200 status code, and a response with invalid JSON.
func TestHandleJobResponse(t *testing.T) {
	user := map[string]string{
		"mail":     "test@example.com",
		"username": "testuser",
	}

	// Test successful response
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		rw.Write([]byte(`{"id": "12345"}`))
	}))
	client := server.Client()
	resp, err := client.Get(server.URL)
	if err != nil {
		t.Errorf("client.get() returned an errror: %v", err)
	}
	defer resp.Body.Close()
	jobId, err := handleJobResponse(resp, user)
	if err != nil {
		t.Errorf("handleJobResponse() returned an error: %v", err)
	}
	if jobId != "12345" {
		t.Errorf("handleJobResponse() returned job ID %v, want 12345", jobId)
	}
	server.Close()

	// Test non-200 status code
	server = httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		rw.WriteHeader(http.StatusInternalServerError)
		rw.Write([]byte(`{"id": "12345"}`))
	}))
	client = server.Client()
	resp, err = client.Get(server.URL)
	if err != nil {
		t.Errorf("client.Get() returned an error: %v", err)
	}
	defer resp.Body.Close()
	_, err = handleJobResponse(resp, user)
	if err == nil {
		t.Errorf("handleJobResponse() did not return an error for non-200 status code")
	}
	server.Close()

	// Test invalid JSON in response
	server = httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		rw.Write([]byte(`invalid JSON`))
	}))
	client = server.Client()
	resp, err = client.Get(server.URL)
	if err != nil {
		t.Errorf("client.Get() returned an error: %v", err)
	}
	defer resp.Body.Close()
	_, err = handleJobResponse(resp, user)
	if err == nil {
		t.Errorf("handleJobResponse() did not return an error for invalid JSON")
	}
	server.Close()
}
