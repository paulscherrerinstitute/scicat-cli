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
	proposal := GetProposal(client, server.URL, "testOwnerGroup", user, []string{"testAccessGroup"})
	
	// Check the proposal
	if proposal["proposal"] != "test proposal" {
		t.Errorf("Expected proposal 'test proposal', got '%s'", proposal["proposal"])
	}
}
