package datasetUtils

import (
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
)

func TestAuthenticate(t *testing.T) {
	// Mock HTTP server
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		rw.Write([]byte(`{"username": "testuser", "accessGroups": ["group1", "group2"]}`))
	}))
	defer server.Close()
	
	// Test cases
	tests := []struct {
		name      string
		token     string
		userpass  string
		wantUser  map[string]string
		wantGroup []string
		}{
			{
				name:     "Test with token",
				token:    "testtoken",
				userpass: "testuser:testpass",
				wantUser: map[string]string{
					"username": "testuser",
					"password": "testpass",
				},
				wantGroup: []string{"group1", "group2"},
			},
			// Add more test cases as needed
		}
		
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			httpClient := server.Client()
			user, group := Authenticate(httpClient, server.URL, &tt.token, &tt.userpass)
			
			if !reflect.DeepEqual(user, tt.wantUser) {
				t.Errorf("got %v, want %v", user, tt.wantUser)
			}
			
			if !reflect.DeepEqual(group, tt.wantGroup) {
				t.Errorf("got %v, want %v", group, tt.wantGroup)
			}
		})
	}
}