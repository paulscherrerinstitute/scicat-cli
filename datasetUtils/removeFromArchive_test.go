package datasetUtils

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"reflect"
	"testing"
)

type JobParams struct {
	Username string `json:"username"`
}

type Payload struct {
	EmailJobInitiator string                   `json:"emailJobInitiator"`
	JobParams         JobParams                `json:"jobParams"`
	JobStatusMessage  string                   `json:"jobStatusMessage"`
	DatasetList       []map[string]interface{} `json:"datasetList"`
	Type              string                   `json:"type"`
}

func TestRemoveFromArchive(t *testing.T) {
	tests := []struct {
		name            string
		mockResponse    string
		expectedDataset []map[string]interface{}
		expectedJobID   string
		expectPost      bool
	}{
		{
			name:            "Return empty datablocks list",
			mockResponse:    `[]`,
			expectedDataset: []map[string]interface{}{},
			expectedJobID:   "",
			expectPost:      false,
		},
		{
			name:         "Return datablocks list of size 2",
			mockResponse: `[{"id": "datablock1", "size": 50}, {"id": "datablock2", "size": 100}]`,
			expectedDataset: []map[string]interface{}{
				{"pid": "dataset1", "files": []interface{}{}},
			},
			expectedJobID: "123",
			expectPost:    true,
		},
	}
	user := map[string]string{
		"mail":        "test@example.com",
		"username":    "testuser",
		"accessToken": "testtoken",
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expectedPayload := Payload{
				EmailJobInitiator: "test@example.com",
				JobParams:         JobParams{Username: "testuser"},
				JobStatusMessage:  "jobSubmitted",
				DatasetList:       tt.expectedDataset,
				Type:              "reset",
			}

			// Create a mock HTTP client
			client := &http.Client{
				Transport: &MockTransport{
					RoundTripFunc: func(req *http.Request) (*http.Response, error) {
						if req.Method == http.MethodGet {
							return &http.Response{
								StatusCode: 200,
								Body:       io.NopCloser(bytes.NewBufferString(tt.mockResponse)),
							}, nil
						}

						if !tt.expectPost {
							t.Fatalf("unexpected POST request when no datablocks are returned")
						}

						body, err := io.ReadAll(req.Body)
						if err != nil {
							t.Fatalf("Failed to read request body: %v", err)
						}
						defer req.Body.Close()

						var actual map[string]interface{}
						json.Unmarshal(body, &actual)

						expectedBytes, _ := json.Marshal(expectedPayload)
						var expected map[string]interface{}
						json.Unmarshal(expectedBytes, &expected)

						delete(actual, "creationTime")

						if !reflect.DeepEqual(actual, expected) {
							t.Errorf("Payload mismatch\nExpected: %+v\nGot: %+v", expected, actual)
						}
						return &http.Response{
							StatusCode: 200,
							Body:       io.NopCloser(bytes.NewBufferString(`{"id": "123"}`)),
						}, nil
					},
				},
			}

			jobID, err := RemoveFromArchive(client, "http://mockserver", "dataset1", user, true, JobParamsStruct{})
			if err != nil {
				t.Fatalf("RemoveFromArchive returned unexpected error: %v", err)
			}

			if jobID != tt.expectedJobID {
				t.Fatalf("Unexpected jobID. Expected: %q, Got: %q", tt.expectedJobID, jobID)
			}
		})
	}
}
