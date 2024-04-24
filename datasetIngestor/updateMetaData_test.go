package datasetIngestor

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestGetAVFromPolicy(t *testing.T) {
	// Test case 1: No policies available
	ts1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`[]`)) // empty policy list
	}))
	defer ts1.Close()
	
	client := ts1.Client()
	
	level := getAVFromPolicy(client, ts1.URL, map[string]string{"accessToken": "testToken"}, "testOwner")
	
	if level != "low" {
		t.Errorf("Expected level to be 'low', got '%s'", level)
	}
	
	// Test case 2: Policies available
	ts2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`[{"TapeRedundancy": "medium", "AutoArchive": false}]`)) // policy list with one policy
	}))
	defer ts2.Close()
	
	client = ts2.Client()
	
	level = getAVFromPolicy(client, ts2.URL, map[string]string{"accessToken": "testToken"}, "testOwner")
	
	if level != "medium" {
		t.Errorf("Expected level to be 'medium', got '%s'", level)
	}
}
