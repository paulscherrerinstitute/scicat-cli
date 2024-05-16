package datasetUtils

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestGetUserInfoFromToken(t *testing.T) {
	// Create a mock server
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		// Send response to be tested
		rw.Write([]byte(`{"currentUser": "testUser", "currentUserEmail": "testUser@example.com", "currentGroups": ["group1", "group2"]}`))
	}))
	// Close the server when test finishes
	defer server.Close()
	
	// Test case: Valid token
	{
		client := server.Client()
		userInfo, groups, _ := GetUserInfoFromToken(client, server.URL, "validToken")
		if userInfo["username"] != "testUser" || userInfo["mail"] != "testUser@example.com" || len(groups) != 2 {
			t.Errorf("GetUserInfoFromToken failed, expected %v, got %v", "testUser", userInfo["username"])
		}
	}
	
	// // Test case: Invalid token
	// // Note: This test case assumes that the server returns a non-200 status code for invalid tokens.
	// {
	// 	client := server.Client()
	// 	defer func() {
	// 	if r := recover(); r == nil {
	// 		t.Errorf("GetUserInfoFromToken did not panic on invalid token")
	// 	}
	// 	}()
	// 	GetUserInfoFromToken(client, server.URL, "invalidToken")
	// }
}
