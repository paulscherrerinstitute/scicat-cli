package datasetUtils

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestGetProposal(t *testing.T) {
	// Create a mock server
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		// Send response to be tested
		rw.Write([]byte(`[{"proposal": "test proposal"}]`))
	}))
	// Close the server when test finishes
	defer server.Close()

	// Create a client
	client := &http.Client{}

	// Create a user
	user := make(map[string]string)
	user["accessToken"] = "testToken"

	// Call GetProposal
	proposal, _ := GetProposal(client, server.URL, "testOwnerGroup", user)

	// Check the proposal
	if proposal["proposal"] != "test proposal" {
		t.Errorf("Expected proposal 'test proposal', got '%s'", proposal["proposal"])
	}
}

func TestGetProposals(t *testing.T) {
	t.Run("returns all proposals and filters by ownerGroup", func(t *testing.T) {
		var gotFilter string
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			gotFilter = req.URL.Query().Get("filters")
			rw.Write([]byte(`[{"proposalId": "p1"}, {"proposalId": "p2"}]`))
		}))
		defer server.Close()

		user := map[string]string{"accessToken": "testToken"}
		proposals, err := GetProposals(server.Client(), server.URL, "testOwnerGroup", user)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(proposals) != 2 || proposals[0]["proposalId"] != "p1" || proposals[1]["proposalId"] != "p2" {
			t.Errorf("unexpected proposals: %v", proposals)
		}
		if want := `{"where":{"ownerGroup":"testOwnerGroup"}}`; gotFilter != want {
			t.Errorf("filter = %s, want %s", gotFilter, want)
		}
	})

	t.Run("returns an empty list when no proposal matches", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			rw.Write([]byte(`[]`))
		}))
		defer server.Close()

		proposals, err := GetProposals(server.Client(), server.URL, "testOwnerGroup", map[string]string{})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(proposals) != 0 {
			t.Errorf("expected no proposals, got %v", proposals)
		}
	})

	t.Run("returns an error on a failing status code", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			rw.WriteHeader(http.StatusForbidden)
		}))
		defer server.Close()

		if _, err := GetProposals(server.Client(), server.URL, "testOwnerGroup", map[string]string{}); err == nil {
			t.Error("expected an error, got nil")
		}
	})
}
