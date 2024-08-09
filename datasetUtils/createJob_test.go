package datasetUtils

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
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
		jobId, err := CreateArchivalJob(client, APIServer, user, datasetList, tapecopies)
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
		jobId, err := CreateArchivalJob(client, APIServer, user, datasetList, tapecopies)
		if err == nil {
			t.Errorf("Expected an error to be returned from CreateJob")
		}

		const expectedError = "CreateJob - request returned unexpected status code: 500"
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
		jobId, err := CreateArchivalJob(client, APIServer, user, datasetList, tapecopies)

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
					json.Unmarshal([]byte(`{"creationTime":"2024-05-21T15:25:34+02:00","datasetList":[{"pid":"dataset1","files":[]},{"pid":"dataset2","files":[]}],"emailJobInitiator":"test@example.com","jobParams":{"tapeCopies":"two","username":"testuser"},"jobStatusMessage":"jobSubmitted","type":"archive"}`), &expectedPayload)

					// Ignore the creationTime field
					delete(actualPayload, "creationTime")
					delete(expectedPayload, "creationTime")

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
		jobId, err := CreateArchivalJob(client, server.URL, user, datasetList, tapecopies)
		if err != nil {
			t.Errorf("Got an error when creating a job: %s", err.Error())
		}

		// Check the result
		if jobId != "123" {
			t.Errorf("Expected jobId to be '123', got '%s'", jobId)
		}
	})
}
