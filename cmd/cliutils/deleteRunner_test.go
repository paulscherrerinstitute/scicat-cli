package cliutils

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/paulscherrerinstitute/scicat-cli/v3/datasetUtils"
)

func TestRunDeletionSuccessSubmitsResetJob(t *testing.T) {
	t.Setenv("TEST_MODE", "true")
	oldAvailabilityURL := datasetUtils.GitHubMainLocation
	t.Cleanup(func() {
		datasetUtils.GitHubMainLocation = oldAvailabilityURL
	})

	var postedJob map[string]interface{}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/cmd/datasetIngestor/datasetIngestorServiceAvailability.yml":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("Production:\n  Ingest:\n    status: on\n  Archive:\n    status: on\nQa:\n  Ingest:\n    status: on\n  Archive:\n    status: on\n"))
		case r.Method == http.MethodGet && r.URL.Path == "/users/my/self":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"id":"u1"}`))
		case r.Method == http.MethodGet && r.URL.Path == "/users/u1/userIdentity":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"profile":{"username":"archiveManager","displayName":"Archivist","accessGroups":["group-a"],"emails":[{"value":"archive@example.com"}]}}`))
		case r.Method == http.MethodGet && r.URL.Path == "/Datablocks":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`[{"id":"db1","size":10}]`))
		case r.Method == http.MethodPost && r.URL.Path == "/Jobs":
			defer r.Body.Close()
			if err := json.NewDecoder(r.Body).Decode(&postedJob); err != nil {
				t.Fatalf("decode request body: %v", err)
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"id":"job-123","jobStatusMessage":"jobSubmitted"}`))
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	datasetUtils.GitHubMainLocation = server.URL

	cfg := IngestorConfig{
		Token:             "token-abc",
		ScicatUrl:         server.URL,
		RequireArchiveMgr: true,
		NonInteractive:    true,
		PID:               "pid-001",
		DeletionCode:      string(datasetUtils.CodeExpired),
		DeletionReason:    "retention elapsed",
	}

	err := RunDeletion(server.Client(), cfg, "0.0.0", "datasetRemover")
	if err != nil {
		t.Fatalf("RunDeletion returned error: %v", err)
	}

	if postedJob == nil {
		t.Fatal("expected reset job submission payload")
	}

	jobParams, ok := postedJob["jobParams"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected jobParams map in payload, got: %#v", postedJob["jobParams"])
	}
	if got := jobParams["username"]; got != "archiveManager" {
		t.Fatalf("expected username archiveManager, got: %v", got)
	}
	if got := jobParams["deletionCode"]; got != string(datasetUtils.CodeExpired) {
		t.Fatalf("expected deletionCode %s, got: %v", datasetUtils.CodeExpired, got)
	}
	if got := jobParams["deletionReason"]; got != "retention elapsed" {
		t.Fatalf("expected deletionReason to be propagated, got: %v", got)
	}
}

func TestRunDeletionRejectsNonArchiveManager(t *testing.T) {
	t.Setenv("TEST_MODE", "true")
	oldAvailabilityURL := datasetUtils.GitHubMainLocation
	t.Cleanup(func() {
		datasetUtils.GitHubMainLocation = oldAvailabilityURL
	})

	datablocksCalled := false

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/cmd/datasetIngestor/datasetIngestorServiceAvailability.yml":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("Production:\n  Ingest:\n    status: on\n  Archive:\n    status: on\n"))
		case r.Method == http.MethodGet && r.URL.Path == "/users/my/self":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"id":"u2"}`))
		case r.Method == http.MethodGet && r.URL.Path == "/users/u2/userIdentity":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"profile":{"username":"normalUser","displayName":"User","accessGroups":[],"emails":[{"value":"user@example.com"}]}}`))
		case r.URL.Path == "/Datablocks":
			datablocksCalled = true
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`[]`))
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	datasetUtils.GitHubMainLocation = server.URL

	err := RunDeletion(server.Client(), IngestorConfig{
		Token:             "token-xyz",
		ScicatUrl:         server.URL,
		RequireArchiveMgr: true,
		NonInteractive:    true,
		PID:               "pid-002",
	}, "0.0.0", "datasetCleaner")

	if err == nil {
		t.Fatal("expected permission denied error")
	}
	if !strings.Contains(err.Error(), "permission denied") {
		t.Fatalf("unexpected error: %v", err)
	}
	if datablocksCalled {
		t.Fatal("expected early return before querying datablocks")
	}
}
