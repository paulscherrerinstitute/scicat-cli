package cmd

import (
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
)

// Create a mock implementation of the interface
type MockAuthenticator struct{}

func (m *MockAuthenticator) AuthenticateUser(httpClient *http.Client, APIServer string, username string, password string) (map[string]string, []string) {
	if username == "" && password == "" {
		return map[string]string{}, []string{}
	}
	return map[string]string{"username": "testuser", "password": "testpass"}, []string{"group1", "group2"}
}

func (m *MockAuthenticator) GetUserInfoFromToken(httpClient *http.Client, APIServer string, token string) (map[string]string, []string) {
	return map[string]string{"username": "tokenuser", "password": "tokenpass"}, []string{"group3", "group4"}
}

func TestAuthenticate(t *testing.T) {
	var auth Authenticator = &MockAuthenticator{}
	noExit := func(v ...any) {

	}
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
			userpass: "",
			wantUser: map[string]string{
				"username": "tokenuser",
				"password": "tokenpass",
			},
			wantGroup: []string{"group3", "group4"},
		},
		{
			name:      "Test with empty token and userpass",
			token:     "",
			userpass:  "",
			wantUser:  map[string]string{},
			wantGroup: []string{},
		},
		{
			name:     "Test with empty token and non-empty userpass",
			token:    "",
			userpass: "testuser:testpass",
			wantUser: map[string]string{
				"username": "testuser",
				"password": "testpass",
			},
			wantGroup: []string{"group1", "group2"},
		},
		{
			name:     "Test with non-empty token and empty userpass",
			token:    "testtoken",
			userpass: "",
			wantUser: map[string]string{
				"username": "tokenuser",
				"password": "tokenpass",
			},
			wantGroup: []string{"group3", "group4"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			httpClient := server.Client()
			user, group := authenticate(auth, httpClient, server.URL, tt.userpass, tt.token, noExit)

			if !reflect.DeepEqual(user, tt.wantUser) {
				t.Errorf("got %v, want %v", user, tt.wantUser)
			}

			if !reflect.DeepEqual(group, tt.wantGroup) {
				t.Errorf("got %v, want %v", group, tt.wantGroup)
			}
		})
	}
}
