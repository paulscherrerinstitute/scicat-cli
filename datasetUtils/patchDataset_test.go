package datasetUtils

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestPatchDataset(t *testing.T) {
	t.Run("sends a PATCH request with the given metadata and succeeds on 200", func(t *testing.T) {
		var gotBody map[string]interface{}
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if req.URL.String() != "/Datasets/testPid" {
				t.Errorf("expected URL '/Datasets/testPid', got '%s'", req.URL.String())
			}
			if req.Method != http.MethodPatch {
				t.Errorf("expected method PATCH, got '%s'", req.Method)
			}
			if req.Header.Get("Content-Type") != "application/json" {
				t.Errorf("expected header 'Content-Type: application/json', got '%s'", req.Header.Get("Content-Type"))
			}
			if req.Header.Get("Authorization") != "Bearer testToken" {
				t.Errorf("expected header 'Authorization: Bearer testToken', got '%s'", req.Header.Get("Authorization"))
			}

			body, _ := io.ReadAll(req.Body)
			if err := json.Unmarshal(body, &gotBody); err != nil {
				t.Fatalf("failed to unmarshal request body: %v", err)
			}

			rw.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		meta := map[string]interface{}{"creationTime": "2024-01-01T00:00:00Z"}
		err := PatchDataset(server.Client(), server.URL, "testToken", "testPid", meta)
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if gotBody["creationTime"] != "2024-01-01T00:00:00Z" {
			t.Errorf("expected creationTime '2024-01-01T00:00:00Z' in request body, got: %v", gotBody)
		}
	})

	t.Run("escapes the dataset id in the URL", func(t *testing.T) {
		var gotEscapedPath string
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			gotEscapedPath = req.URL.EscapedPath()
			rw.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		err := PatchDataset(server.Client(), server.URL, "testToken", "some/pid", map[string]interface{}{})
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if gotEscapedPath != "/Datasets/some%2Fpid" {
			t.Errorf("expected escaped dataset id in path, got: %s", gotEscapedPath)
		}
	})

	t.Run("returns an error when the server responds with a non-2xx status", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			rw.WriteHeader(http.StatusNotFound)
		}))
		defer server.Close()

		err := PatchDataset(server.Client(), server.URL, "testToken", "testPid", map[string]interface{}{})
		if err == nil {
			t.Fatal("expected an error, got nil")
		}
	})
}
