package datasetUtils

import (
	"net/http"
	"net/http/httptest"
	"testing"
	
	"github.com/stretchr/testify/assert"
)

// This test creates a mock server that responds with a JSON object when it receives a GET request. It then calls GetJson with the URL of the mock server and checks that the function correctly decodes the response into the target variable.
func TestGetJson(t *testing.T) {
	// Create a mock server
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		rw.Write([]byte(`{"fakeKey": "fakeValue"}`))
	}))
	// Close the server when test finishes
	defer server.Close()
	
	// Create a client
	client := &http.Client{}
	
	// Create a target to hold our expected data
	var target map[string]string
	
	// Send a request to our mock server
	err := GetJson(client, server.URL, &target)
	
	// Assert there was no error
	assert.Nil(t, err)
	
	// Assert the target is correctly populated
	assert.Equal(t, "fakeValue", target["fakeKey"])
}
