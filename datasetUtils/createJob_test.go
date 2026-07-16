package datasetUtils

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"testing"
)

type MockTransport struct {
	RoundTripFunc func(req *http.Request) (*http.Response, error)
}

func (m *MockTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return m.RoundTripFunc(req)
}

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
		jobId, err := CreateArchivalJob(client, APIServer, user, "group1", datasetList, tapecopies, nil)
		if err != nil {
			t.Errorf("Unexpected error received: %v", err)
		}

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
		jobId, err := CreateArchivalJob(client, APIServer, user, "group1", datasetList, tapecopies, nil)
		if err == nil {
			t.Errorf("Expected an error to be returned from CreateJob")
		}

		const expectedError = "CreateJob - request returned error status code: 500, body: "
		if err.Error() != expectedError {
			t.Errorf("Got incorrect error from CreateJob - expected: \"%s\", gotten: \"%s\"", expectedError, err.Error())
		}

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
		jobId, err := CreateArchivalJob(client, APIServer, user, "group1", datasetList, tapecopies, nil)

		if err == nil {
			t.Error("Expected an error to be returned from CreateJob")
		}

		const expectedError = "CreateJob - could not decode id from job: invalid character 'i' looking for beginning of value"
		if err.Error() != expectedError {
			t.Errorf("Got incorrect error from CreateJob - expected: \"%s\", gotten: \"%s\"", expectedError, err.Error())
		}

		// Check the result
		if jobId != "" {
			t.Errorf("Expected jobId to be '', got '%s'", jobId)
		}
	})

	t.Run("client.Do called with expected payload", func(t *testing.T) {
		// Create a mock server
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			rw.Write([]byte(`{"id": "123"}`))
		}))
		defer server.Close()

		user := map[string]string{
			"mail":        "test@example.com",
			"username":    "testuser",
			"accessToken": "testtoken",
		}
		datasetList := []string{"dataset1", "dataset2"}
		tapecopies := new(int)
		*tapecopies = 2

		// Create a mock client
		client := &http.Client{
			Transport: &MockTransport{
				RoundTripFunc: func(req *http.Request) (*http.Response, error) {
					body, _ := io.ReadAll(req.Body)

					// Parse the actual and expected payloads
					var actualPayload, expectedPayload map[string]interface{}
					json.Unmarshal(body, &actualPayload)
					json.Unmarshal([]byte(`
					{
					  "emailJobInitiator": "test@example.com",
					  "jobParams": {
					    "tapeCopies": "two",
					    "username": "testuser",
					    "ownerGroup": "group1"
					  },
					  "jobStatusMessage": "jobSubmitted",
					  "datasetList": [
					    {
					      "pid": "dataset1",
					      "files": []
					    },
					    {
					      "pid": "dataset2",
					      "files": []
					    }
					  ],
					  "type": "archive",
					  "executionTime": null
					}`), &expectedPayload)

					// Ignore the creationTime field
					delete(actualPayload, "creationTime")

					// Check if the payloads match
					if !reflect.DeepEqual(actualPayload, expectedPayload) {
						t.Errorf("Expected payload to be '%v', got '%v'", expectedPayload, actualPayload)
					}

					// We still need to return a response
					return &http.Response{
						StatusCode: 200,
						Body:       io.NopCloser(bytes.NewBufferString(`{"id": "123"}`)),
					}, nil
				},
			},
		}

		// Call the function with the mock client
		jobId, err := CreateArchivalJob(client, server.URL, user, "group1", datasetList, tapecopies, nil)
		if err != nil {
			t.Errorf("Got an error when creating a job: %s", err.Error())
		}

		// Check the result
		if jobId != "123" {
			t.Errorf("Expected jobId to be '123', got '%s'", jobId)
		}
	})
}

func TestGroupDatasetsByOwnerGroup(t *testing.T) {
	t.Run("groups datasets correctly", func(t *testing.T) {
		datasetList := []string{"ds1", "ds2", "ds3", "ds4"}
		ownerGroupList := []string{"group1", "group2", "group1", "group2"}

		groupedDatasets, err := GroupDatasetsByOwnerGroup(datasetList, ownerGroupList)
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}

		if len(groupedDatasets["group1"]) != 2 {
			t.Errorf("Expected 2 datasets in group1, got %d", len(groupedDatasets["group1"]))
		}
		if !slices.Contains(groupedDatasets["group1"], "ds1") || !slices.Contains(groupedDatasets["group1"], "ds3") {
			t.Errorf("group1 has incorrect datasets: %v", groupedDatasets["group1"])
		}

		if len(groupedDatasets["group2"]) != 2 {
			t.Errorf("Expected 2 datasets in group2, got %d", len(groupedDatasets["group2"]))
		}
		if !slices.Contains(groupedDatasets["group2"], "ds2") || !slices.Contains(groupedDatasets["group2"], "ds4") {
			t.Errorf("group2 has incorrect datasets: %v", groupedDatasets["group2"])
		}
	})

	t.Run("mismatched list lengths returns error", func(t *testing.T) {
		datasetList := []string{"ds1", "ds2"}
		ownerGroupList := []string{"group1"}

		_, err := GroupDatasetsByOwnerGroup(datasetList, ownerGroupList)
		if err == nil {
			t.Error("Expected error when dataset and owner group lists have different lengths")
		}

		expectedError := "datasetList and ownerGroupList are not the same length"
		if err.Error() != expectedError {
			t.Errorf("Got incorrect error - expected: \"%s\", gotten: \"%s\"", expectedError, err.Error())
		}
	})

	t.Run("single group", func(t *testing.T) {
		datasetList := []string{"ds1", "ds2", "ds3"}
		ownerGroupList := []string{"group1", "group1", "group1"}

		groupedDatasets, err := GroupDatasetsByOwnerGroup(datasetList, ownerGroupList)
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}

		if len(groupedDatasets) != 1 {
			t.Errorf("Expected 1 group, got %d", len(groupedDatasets))
		}

		if len(groupedDatasets["group1"]) != 3 {
			t.Errorf("Expected 3 datasets in group1, got %d", len(groupedDatasets["group1"]))
		}
	})
}

func TestCreateArchivalJobs(t *testing.T) {
	t.Run("creates jobs for multiple groups", func(t *testing.T) {
		callCount := 0
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			callCount++
			var body map[string]interface{}
			json.NewDecoder(req.Body).Decode(&body)

			jobParams := body["jobParams"].(map[string]interface{})
			ownerGroup := jobParams["ownerGroup"].(string)

			if ownerGroup == "group1" {
				rw.Write([]byte(`{"id": "job-group1"}`))
			} else if ownerGroup == "group2" {
				rw.Write([]byte(`{"id": "job-group2"}`))
			}
		}))
		defer server.Close()

		client := server.Client()
		user := map[string]string{
			"mail":        "test@example.com",
			"username":    "testuser",
			"accessToken": "testtoken",
		}
		groupedDatasets := map[string][]string{
			"group1": {"ds1", "ds2"},
			"group2": {"ds3", "ds4"},
		}
		tapecopies := new(int)
		*tapecopies = 1

		jobIds, errs := CreateArchivalJobs(client, server.URL, user, groupedDatasets, tapecopies)

		if len(jobIds) != 2 {
			t.Errorf("Expected 2 job IDs, got %d", len(jobIds))
		}

		for i, err := range errs {
			if err != nil {
				t.Errorf("Unexpected error at index %d: %v", i, err)
			}
		}

		nonEmptyIds := 0
		for _, id := range jobIds {
			if id != "" {
				nonEmptyIds++
			}
		}
		if nonEmptyIds != 2 {
			t.Errorf("Expected 2 non-empty job IDs, got %d: %v", nonEmptyIds, jobIds)
		}

		if callCount != 2 {
			t.Errorf("Expected 2 server calls, got %d", callCount)
		}
	})

	t.Run("handles errors in job creation", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			rw.WriteHeader(http.StatusInternalServerError)
			rw.Write([]byte(`Internal Server Error`))
		}))
		defer server.Close()

		client := server.Client()
		user := map[string]string{
			"mail":        "test@example.com",
			"username":    "testuser",
			"accessToken": "testtoken",
		}
		groupedDatasets := map[string][]string{
			"group1": {"ds1"},
		}
		tapecopies := new(int)
		*tapecopies = 1

		_, errs := CreateArchivalJobs(client, server.URL, user, groupedDatasets, tapecopies)

		if len(errs) == 0 {
			t.Error("Expected at least one error")
		}

		hasError := false
		for _, err := range errs {
			if err != nil {
				hasError = true
				break
			}
		}
		if !hasError {
			t.Error("Expected at least one non-nil error")
		}
	})
}
