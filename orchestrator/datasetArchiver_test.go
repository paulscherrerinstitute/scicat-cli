package orchestrator

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestResolveArchivableDatasets(t *testing.T) {
	t.Run("resolves via ownerGroup", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			fmt.Fprint(rw, `[{"pid":"1","sourceFolder":"folder1","size":10},{"pid":"2","sourceFolder":"folder2","size":0}]`)
		}))
		defer server.Close()

		datasets, err := ResolveArchivableDatasets(server.Client(), server.URL, "testToken", "testGroup", nil)
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if len(datasets) != 1 || datasets[0] != "1" {
			t.Errorf("expected [\"1\"], got %v", datasets)
		}
	})

	t.Run("resolves via datasetIds when all exist and are archivable", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			fmt.Fprint(rw, `[{"pid":"1","sourceFolder":"folder1","size":10},{"pid":"2","sourceFolder":"folder2","size":10}]`)
		}))
		defer server.Close()

		datasets, err := ResolveArchivableDatasets(server.Client(), server.URL, "testToken", "", []string{"1", "2"})
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if len(datasets) != 2 || datasets[0] != "1" || datasets[1] != "2" {
			t.Errorf("expected [\"1\" \"2\"], got %v", datasets)
		}
	})

	t.Run("fails when some datasetIds are missing or not archivable", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			fmt.Fprint(rw, `[{"pid":"1","sourceFolder":"folder1","size":10}]`)
		}))
		defer server.Close()

		_, err := ResolveArchivableDatasets(server.Client(), server.URL, "testToken", "", []string{"1", "2"})
		if err == nil {
			t.Fatal("expected an error, got nil")
		}
	})

	t.Run("fails when neither ownerGroup nor datasetIds are set", func(t *testing.T) {
		_, err := ResolveArchivableDatasets(http.DefaultClient, "", "testToken", "", nil)
		if err == nil {
			t.Fatal("expected an error, got nil")
		}
	})

	t.Run("fails when no archivable datasets remain", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			fmt.Fprint(rw, `[{"pid":"1","sourceFolder":"folder1","size":0}]`)
		}))
		defer server.Close()

		_, err := ResolveArchivableDatasets(server.Client(), server.URL, "testToken", "testGroup", nil)
		if err == nil {
			t.Fatal("expected an error, got nil")
		}
	})
}

func TestResolveOwnerGroup(t *testing.T) {
	t.Run("uses the explicit ownerGroup when set", func(t *testing.T) {
		group, err := ResolveOwnerGroup("testGroup", []string{"group1", "group2"})
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if group != "testGroup" {
			t.Errorf("expected \"testGroup\", got %v", group)
		}
	})

	t.Run("falls back to the first accessGroup when ownerGroup is empty", func(t *testing.T) {
		group, err := ResolveOwnerGroup("", []string{"group1", "group2"})
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if group != "group1" {
			t.Errorf("expected \"group1\", got %v", group)
		}
	})

	t.Run("fails when ownerGroup is empty and there are no accessGroups", func(t *testing.T) {
		_, err := ResolveOwnerGroup("", nil)
		if err == nil {
			t.Fatal("expected an error, got nil")
		}
	})
}

func TestParseExecutionTime(t *testing.T) {
	t.Run("empty string returns nil", func(t *testing.T) {
		parsed, err := ParseExecutionTime("")
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if parsed != nil {
			t.Errorf("expected nil, got: %v", parsed)
		}
	})

	t.Run("valid RFC3339 timestamp is parsed", func(t *testing.T) {
		parsed, err := ParseExecutionTime("2026-07-13T10:00:00Z")
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		want := time.Date(2026, 7, 13, 10, 0, 0, 0, time.UTC)
		if parsed == nil || !parsed.Equal(want) {
			t.Errorf("expected %v, got %v", want, parsed)
		}
	})

	t.Run("invalid timestamp returns an error", func(t *testing.T) {
		_, err := ParseExecutionTime("not-a-time")
		if err == nil {
			t.Fatal("expected an error, got nil")
		}
	})
}
