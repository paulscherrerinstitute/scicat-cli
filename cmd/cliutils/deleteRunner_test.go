package cliutils

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/paulscherrerinstitute/scicat-cli/v3/datasetUtils"
)

func TestRunArchiveOnlyRemovalSuccess(t *testing.T) {
	t.Setenv("TEST_MODE", "true")
	oldAvailabilityURL := datasetUtils.GitHubMainLocation
	t.Cleanup(func() {
		datasetUtils.GitHubMainLocation = oldAvailabilityURL
	})

	var postedJob map[string]interface{}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "ServiceAvailability.yml"):
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("Production:\n  Ingest:\n    status: on\n  Archive:\n    status: on\n"))
		case r.Method == http.MethodGet && r.URL.Path == "/users/my/self":
			_, _ = w.Write([]byte(`{"id":"u1"}`))
		case r.Method == http.MethodGet && r.URL.Path == "/users/u1/userIdentity":
			_, _ = w.Write([]byte(`{"profile":{"username":"aUser","emails":[{"value":"aUser@example.com"}]}}`))
		case r.Method == http.MethodGet && r.URL.Path == "/Datablocks":
			_, _ = w.Write([]byte(`[{"id":"db1"}]`))
		case r.Method == http.MethodPost && r.URL.Path == "/Jobs":
			defer r.Body.Close()
			if err := json.NewDecoder(r.Body).Decode(&postedJob); err != nil {
				t.Fatalf("failed to decode posted job: %v", err)
			}
			_, _ = w.Write([]byte(`{"id":"job-123"}`))
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	datasetUtils.GitHubMainLocation = server.URL

	cfg := RemoveConfig{
		BaseConfig: BaseConfig{
			Token:      "token-abc",
			HttpClient: server.Client(),
			EnvConfig: InputEnvironmentConfig{
				ScicatUrl: server.URL,
			},
			NonInteractive: true,
		},
		DeletionCode:   string(datasetUtils.CodeExpired),
		DeletionReason: "retention elapsed",
	}

	err := cfg.RunArchiveOnlyRemoval("pid-001", "0.0.0", "datasetRemover")
	if err != nil {
		t.Fatalf("RunArchiveOnlyRemoval returned error: %v", err)
	}

	if postedJob == nil {
		t.Fatal("expected job submission payload")
	}

	jobParams, ok := postedJob["jobParams"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected jobParams map, got %#v", postedJob["jobParams"])
	}
	if got := jobParams["deletionCode"]; got != string(datasetUtils.CodeExpired) {
		t.Fatalf("expected deletionCode %s, got: %v", datasetUtils.CodeExpired, got)
	}
	if got := jobParams["deletionReason"]; got != "retention elapsed" {
		t.Fatalf("expected deletionReason retention elapsed, got: %v", got)
	}
}

func TestRunFullRemoval(t *testing.T) {
	t.Setenv("TEST_MODE", "true")
	oldAvailabilityURL := datasetUtils.GitHubMainLocation
	t.Cleanup(func() {
		datasetUtils.GitHubMainLocation = oldAvailabilityURL
	})

	testCases := []struct {
		name              string
		username          string
		removeFromCatalog bool
		expectError       string
		wantArchive       bool
		wantCatalog       bool
	}{
		{
			name:              "Error: Not an Archive Manager",
			username:          "normalUser",
			removeFromCatalog: true,
			expectError:       "permission denied",
			wantArchive:       false,
			wantCatalog:       false,
		},
		{
			name:              "Success: Archive Only",
			username:          "archiveManager",
			removeFromCatalog: false,
			expectError:       "",
			wantArchive:       true,
			wantCatalog:       false,
		},
		{
			name:              "Success: Full Removal",
			username:          "archiveManager",
			removeFromCatalog: true,
			expectError:       "",
			wantArchive:       true,
			wantCatalog:       true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var archiveCalled, catalogCalled bool

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch {
				case strings.HasSuffix(r.URL.Path, "ServiceAvailability.yml"):
					w.WriteHeader(http.StatusOK)
					_, _ = w.Write([]byte("Production:\n  Ingest:\n    status: on\n  Archive:\n    status: on\n"))
				case r.URL.Path == "/users/my/self":
					_, _ = w.Write([]byte(`{"id":"u1"}`))
				case r.URL.Path == "/users/u1/userIdentity":
					_, _ = w.Write([]byte(`{"profile":{"username":"` + tc.username + `","emails":[{"value":"archive@example.com"}]}}`))
				case r.Method == http.MethodGet && r.URL.Path == "/Datablocks":
					_, _ = w.Write([]byte(`[{"id":"db1"}]`))
				case r.Method == http.MethodPost && r.URL.Path == "/Jobs":
					archiveCalled = true
					_, _ = w.Write([]byte(`{"id":"job-1","jobStatusMessage":"jobSubmitted"}`))
				case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/origdatablocks/count"):
					_, _ = w.Write([]byte(`{"count":1}`))
				case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/attachments/count"):
					_, _ = w.Write([]byte(`{"count":1}`))
				case r.Method == http.MethodGet && r.URL.Path == "/Datasets/count":
					_, _ = w.Write([]byte(`{"count":1}`))
				case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/datablocks/count"):
					_, _ = w.Write([]byte(`{"count":0}`))
				case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/Jobs/"):
					_, _ = w.Write([]byte(`{"id":"job-1","jobStatusMessage":"finishedSuccessful"}`))
				case r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, "/Datasets/"):
					catalogCalled = true
					w.WriteHeader(http.StatusOK)
				default:
					t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
				}
			}))
			defer server.Close()

			datasetUtils.GitHubMainLocation = server.URL

			cfg := CleanConfig{
				BaseConfig: BaseConfig{
					Token:          "token-abc",
					HttpClient:     server.Client(),
					EnvConfig:      InputEnvironmentConfig{ScicatUrl: server.URL},
					NonInteractive: true,
				},
				RemoveFromCatalog: tc.removeFromCatalog,
			}

			err := cfg.RunFullRemoval("pid-123", "0.0.0", "test")

			if tc.expectError != "" {
				if err == nil || !strings.Contains(err.Error(), tc.expectError) {
					t.Fatalf("expected error %q, got %v", tc.expectError, err)
				}
			} else if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if archiveCalled != tc.wantArchive {
				t.Errorf("Archive call mismatch: got %v, want %v", archiveCalled, tc.wantArchive)
			}
			if catalogCalled != tc.wantCatalog {
				t.Errorf("Catalog call mismatch: got %v, want %v", catalogCalled, tc.wantCatalog)
			}
		})
	}
}
