package datasetUtils

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"testing"
)

func TestPatchJobStatus(t *testing.T) {
	tests := []struct {
		name                   string
		mockResponseStatusCode int
	}{
		{
			name:                   "Return 200 OK",
			mockResponseStatusCode: 200,
		},
		{
			name:                   "Return 404 error",
			mockResponseStatusCode: 404,
		},
	}
	user := map[string]string{
		"mail":        "test@example.com",
		"username":    "testuser",
		"accessToken": "testtoken",
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a mock HTTP client
			client := &http.Client{
				Transport: &MockTransport{
					RoundTripFunc: func(req *http.Request) (*http.Response, error) {
						if req.Method != "PATCH" {
							t.Fatalf("Expected PATCH method, got %s", req.Method)
						}

						body, err := io.ReadAll(req.Body)
						if err != nil {
							t.Fatalf("Failed to read request body: %v", err)
						}
						defer req.Body.Close()

						var actual map[string]interface{}
						json.Unmarshal(body, &actual)

						return &http.Response{
							StatusCode: tt.mockResponseStatusCode,
							Body:       io.NopCloser(bytes.NewBufferString("")),
						}, nil
					},
				},
			}

			err := PatchJobStatus(client, "http://mockserver", user, "123", "Completed")
			if err != nil && tt.mockResponseStatusCode < 400 {
				t.Fatalf("PatchJobStatus returned unexpected error: %v", err)
			}

			if err != nil && tt.mockResponseStatusCode >= 400 {
				expectedError := "job status request failed"
				if !bytes.Contains([]byte(err.Error()), []byte(expectedError)) {
					t.Fatalf("Expected error to contain '%s', got '%s'", expectedError, err.Error())
				}
			}

			if err == nil && tt.mockResponseStatusCode >= 400 {
				t.Fatalf("Expected error for status code %d, got nil", tt.mockResponseStatusCode)
			}
		})
	}
}
