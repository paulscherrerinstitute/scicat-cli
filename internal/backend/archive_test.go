package backend

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestNewArchiveService(t *testing.T) {
	base := &TransportEngine{}

	svc := NewArchiveService(base)

	if svc == nil {
		t.Fatalf("expected service to be initialized")
	}
	if svc.Base != base {
		t.Fatalf("expected service to keep provided base transport")
	}
}

func TestSubmitArchivalJobSkipsWhenNoDatasetIDs(t *testing.T) {
	// Base can be nil because the method should return before dereferencing it.
	svc := &ArchiveService{Base: nil}

	jobID, err := svc.SubmitArchivalJob("p12345", nil, 1)
	if err != nil {
		t.Fatalf("expected no error for empty dataset list, got: %v", err)
	}
	if jobID != "" {
		t.Fatalf("expected empty job id for empty dataset list, got: %q", jobID)
	}
}

func TestSubmitArchivalJobSuccess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("expected method POST, got %s", r.Method)
		}
		if r.URL.Path != "/jobs" {
			t.Fatalf("expected path /jobs, got %s", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer token-123" {
			t.Fatalf("expected auth header Bearer token-123, got %q", got)
		}

		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("failed reading request body: %v", err)
		}

		var payload map[string]interface{}
		if err := json.Unmarshal(body, &payload); err != nil {
			t.Fatalf("failed decoding request body: %v", err)
		}

		if payload["type"] != "archive" {
			t.Fatalf("expected type=archive, got %v", payload["type"])
		}
		if payload["jobStatusMessage"] != "jobSubmitted" {
			t.Fatalf("expected jobStatusMessage=jobSubmitted, got %v", payload["jobStatusMessage"])
		}

		jobParams, ok := payload["jobParams"].(map[string]interface{})
		if !ok {
			t.Fatalf("expected jobParams object")
		}
		if jobParams["ownerGroup"] != "p12345" {
			t.Fatalf("expected ownerGroup=p12345, got %v", jobParams["ownerGroup"])
		}
		if jobParams["tapeCopies"] != "two" {
			t.Fatalf("expected tapeCopies=two for tapeCopies=2 input, got %v", jobParams["tapeCopies"])
		}

		datasetList, ok := payload["datasetList"].([]interface{})
		if !ok {
			t.Fatalf("expected datasetList array")
		}
		if len(datasetList) != 2 {
			t.Fatalf("expected 2 dataset entries, got %d", len(datasetList))
		}

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"id":"job-42"}`))
	}))
	defer server.Close()

	base := &TransportEngine{
		Client:    server.Client(),
		APIServer: server.URL,
		User: map[string]string{
			"accessToken": "token-123",
			"username":    "alice",
			"mail":        "alice@example.org",
		},
	}
	svc := NewArchiveService(base)

	jobID, err := svc.SubmitArchivalJob("p12345", []string{"pid-1", "pid-2"}, 2)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if jobID != "job-42" {
		t.Fatalf("expected job id job-42, got: %q", jobID)
	}
}

func TestSubmitArchivalJobReturnsErrorOnBackendFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":"boom"}`))
	}))
	defer server.Close()

	base := &TransportEngine{
		Client:    server.Client(),
		APIServer: server.URL,
		User: map[string]string{
			"accessToken": "token-123",
			"username":    "alice",
			"mail":        "alice@example.org",
		},
	}
	svc := NewArchiveService(base)

	jobID, err := svc.SubmitArchivalJob("p12345", []string{"pid-1"}, 1)
	if err == nil {
		t.Fatalf("expected error when backend returns non-2xx status")
	}
	if jobID != "" {
		t.Fatalf("expected empty job id on failure, got: %q", jobID)
	}
}

func TestSubmitArchivalJobReturnsErrorWhenOwnerGroupMissing(t *testing.T) {
	base := &TransportEngine{
		Client:    &http.Client{},
		APIServer: "http://example.invalid",
		User: map[string]string{
			"accessToken": "token-123",
			"username":    "alice",
			"mail":        "alice@example.org",
		},
	}
	svc := NewArchiveService(base)

	jobID, err := svc.SubmitArchivalJob("", []string{"pid-1"}, 1)
	if err == nil {
		t.Fatalf("expected error when owner group is missing")
	}
	if jobID != "" {
		t.Fatalf("expected empty job id on failure, got: %q", jobID)
	}
}
