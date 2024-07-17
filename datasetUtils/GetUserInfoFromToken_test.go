package datasetUtils

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestGetUserInfoFromToken(t *testing.T) {
	// Test case: Valid token and user is found
	{
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			rw.Write([]byte(`{"currentUser": "testUser", "currentUserEmail": "testUser@example.com", "currentGroups": ["group1", "group2"]}`))
		}))
		defer server.Close()
		
		client := server.Client()
		userInfo, groups, err := GetUserInfoFromToken(client, server.URL, "validToken")
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		if userInfo["username"] != "testUser" || userInfo["mail"] != "testUser@example.com" || len(groups) != 2 {
			t.Errorf("GetUserInfoFromToken failed, expected %v, got %v", "testUser", userInfo["username"])
		}
	}

	// Test case: Server returns valid response but user is not found
	{
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			rw.Write([]byte(`{"currentUser": "", "currentUserEmail": "", "currentGroups": []}`))
		}))
		defer server.Close()
		
		client := server.Client()
		_, _, err := GetUserInfoFromToken(client, server.URL, "validToken")
		if err == nil {
			t.Errorf("Expected error for user not found, got nil")
		}
	}

	// Test case: Server returns invalid JSON
	{
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			rw.Write([]byte(`invalid JSON`))
		}))
		defer server.Close()
		
		client := server.Client()
		_, _, err := GetUserInfoFromToken(client, server.URL, "validToken")
		if err == nil {
			t.Errorf("Expected error for invalid JSON, got nil")
		}
	}
	
	// Test case: Server returns non-200 status code
	{
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			rw.WriteHeader(http.StatusUnauthorized)
		}))
		defer server.Close()
		
		client := server.Client()
		_, _, err := GetUserInfoFromToken(client, server.URL, "invalidToken")
		if err == nil {
			t.Errorf("Expected error for non-200 status code, got nil")
		}
	}
}
