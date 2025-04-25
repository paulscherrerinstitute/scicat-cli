package datasetUtils

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestAuthenticateUser(t *testing.T) {
	type LoginBody struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}

	var currentUsername, currentDisplayName, currentPassword, currentToken string
	var groupsToReturn []string

	timeStamp := time.Now().Format(time.RFC3339)

	// mock controller for POST /auth/login
	loginController := func(rw http.ResponseWriter, req *http.Request, ldapLogin bool) {
		body, err := io.ReadAll(req.Body)
		if err != nil {
			t.Errorf("Received error when reading body: %s", err.Error())
			rw.Header().Set("Content-Type", "plain/text")
			rw.WriteHeader(400)
			rw.Write([]byte("body reading error"))
			return
		}

		var login LoginBody
		err = json.Unmarshal(body, &login)
		if err != nil {
			rw.WriteHeader(400)
			rw.Write([]byte("invalid json"))
			return
		}

		if login.Username != currentUsername || login.Password != currentPassword {
			rw.Header().Set("Content-Type", "plain/text")
			rw.WriteHeader(401)
			rw.Write([]byte("invalid username or password"))
			return
		}

		authStrat := "local"
		if ldapLogin {
			authStrat = "ldap"
		}

		rw.Header().Set("Content-Type", "application/json")
		rw.WriteHeader(201)
		rw.Write([]byte(`{
  "access_token": "` + currentToken + `",
  "id": "example_id",
  "expires_in": 3600,
  "ttl": 3600,
  "created": "` + timeStamp + `",
  "userId": "example_user_id",
  "user": {
    "_id": "example_user_id",
    "username": "` + currentUsername + `",
    "email": "` + currentUsername + `@your.site",
    "authStrategy": "` + authStrat + `",
    "__v": 0,
    "id": "example_user_id"
  }
}`))
	}

	// mock identity controller for GET /users/my/identity
	identityController := func(rw http.ResponseWriter, req *http.Request) {
		type identityResponse struct {
			Profile struct {
				Username     string   `json:"username"`
				DisplayName  string   `json:"displayName"`
				Email        string   `json:"email"`
				AccessGroups []string `json:"accessGroups"`
			} `json:"profile"`
		}

		token := req.Header.Get("Authorization")
		if token != "Bearer "+currentToken {
			rw.Header().Set("Content-Type", "plain/text")
			rw.WriteHeader(401)
			rw.Write([]byte("Invalid token"))
			return
		}
		resp := identityResponse{}
		resp.Profile.Username = currentUsername
		resp.Profile.DisplayName = currentDisplayName
		resp.Profile.Email = currentUsername + "@your.site"
		resp.Profile.AccessGroups = groupsToReturn

		respJson, err := json.Marshal(resp)
		if err != nil {
			t.Errorf("can't marshal json: %s", err.Error())
			rw.Header().Set("Content-Type", "plain/text")
			rw.WriteHeader(500)
			rw.Write([]byte("marshaling error"))
			return
		}

		rw.Header().Set("Content-Type", "application/json")
		rw.WriteHeader(200)
		rw.Write(respJson)
	}

	// mock controller for

	// mock HTTP server
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		defer req.Body.Close()
		if req.Method == "POST" && strings.HasSuffix(req.URL.String(), "/auth/login") {
			loginController(rw, req, false)
			return
		}
		if req.Method == "POST" && strings.HasSuffix(req.URL.String(), "/auth/ldap") {
			loginController(rw, req, true)
			return
		}
		if req.Method == "GET" && strings.HasSuffix(req.URL.String(), "/users/my/identity") {
			identityController(rw, req)
			return
		}
		// give error if it's not one of those two endpoints
		t.Errorf("function used unexpected method or endpoint: %s - %s", req.Method, req.URL.String())
		rw.Header().Set("Content-Type", "plain/text")
		rw.WriteHeader(404)
		rw.Write([]byte("endpoint does not exist"))
	}))
	_ = server

	// Test cases
	tests := []struct {
		testName string

		usedUsername string
		usedPass     string
		usedToken    string

		currentUsername    string
		currentDisplayName string
		currentPass        string
		currentToken       string
		groupsReturned     []string

		useToken    bool
		ldapLogin   bool
		wantError   string
		wantUserMap map[string]string
	}{
		{
			testName: "Basic user and pass",

			usedUsername: "user",
			usedPass:     "password",

			currentUsername:    "user",
			currentDisplayName: "Some Name",
			currentPass:        "password",
			currentToken:       "sometoken",
			groupsReturned:     []string{"group1", "group2"},

			useToken:  false,
			ldapLogin: false,
			wantError: "",
			wantUserMap: map[string]string{
				"username":    "user",
				"mail":        "user@your.site",
				"displayName": "Some Name",
				"accessToken": "sometoken",
				"expiresIn":   "3600",
				"created":     timeStamp,
				"password":    "password",
			},
		},
		{
			testName: "wrong password",

			usedUsername: "user",
			usedPass:     "passwordbad",

			currentUsername:    "user",
			currentDisplayName: "Some Name",
			currentPass:        "password",
			currentToken:       "sometoken",
			groupsReturned:     []string{"group1", "group2"},

			useToken:  false,
			ldapLogin: false,
			wantError: "error when logging in: 'invalid username or password'",
		},
		{
			testName: "wrong username",

			usedUsername: "userbad",
			usedPass:     "password",

			currentUsername:    "user",
			currentDisplayName: "Some Name",
			currentPass:        "password",
			currentToken:       "sometoken",
			groupsReturned:     []string{"group1", "group2"},

			useToken:  false,
			ldapLogin: false,
			wantError: "error when logging in: 'invalid username or password'",
		},
	}
	for _, test := range tests {
		currentUsername = test.currentUsername
		currentDisplayName = test.currentDisplayName
		currentPassword = test.currentPass
		currentToken = test.currentToken
		groupsToReturn = test.groupsReturned

		t.Run(test.testName, func(t *testing.T) {
			user, group, err := AuthenticateUser(server.Client(), server.URL, test.usedUsername, test.usedPass, test.ldapLogin)

			if test.wantError != "" {
				if err == nil {
					t.Errorf("no error was returned for wrong input")
				} else if err.Error() != test.wantError {
					t.Errorf("wrong error returned - got \"%s\", want \"%s\"", err.Error(), test.wantError)
				}
				return
			}

			if err != nil {
				t.Errorf("authenticate returned an error: %s", err.Error())
			}

			if !reflect.DeepEqual(user, test.wantUserMap) {
				t.Errorf("got %v, want %v", user, test.wantUserMap)
			}

			if !reflect.DeepEqual(group, test.groupsReturned) {
				t.Errorf("got %v, want %v", group, test.groupsReturned)
			}
		})
	}
	_ = tests
}
