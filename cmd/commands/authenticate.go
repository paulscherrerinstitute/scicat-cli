package cmd

import (
	"fmt"
	"log"
	"net/http"
	"strings"
	"syscall"

	"github.com/paulscherrerinstitute/scicat/datasetUtils"
	"golang.org/x/term"
)

// An interface with the methods so that we can mock them in tests
type Authenticator interface {
	AuthenticateUser(httpClient *http.Client, APIServer string, username string, password string) (map[string]string, []string)
	GetUserInfoFromToken(httpClient *http.Client, APIServer string, token string) (map[string]string, []string)
}

type RealAuthenticator struct{}

func (r RealAuthenticator) AuthenticateUser(httpClient *http.Client, APIServer string, username string, password string) (map[string]string, []string) {
	return datasetUtils.AuthenticateUser(httpClient, APIServer, username, password)
}

func (r RealAuthenticator) GetUserInfoFromToken(httpClient *http.Client, APIServer string, token string) (map[string]string, []string) {
	return datasetUtils.GetUserInfoFromToken(httpClient, APIServer, token)
}

// Authenticate handles user authentication by prompting the user for their credentials,
// validating these credentials against the authentication server,
// and returning an authentication token if the credentials are valid.
// This token can then be used for authenticated requests to the server.
// If the credentials are not valid, the function returns an error.
func authenticate(authenticator Authenticator, httpClient *http.Client, apiServer string, userpass string, token string, overrideFatalExit ...func(v ...any)) (map[string]string, []string) {
	fatalExit := log.Fatal // by default, call log fatal
	if len(overrideFatalExit) == 1 {
		fatalExit = overrideFatalExit[0]
	}
	if token != "" {
		user, accessGroups := authenticator.GetUserInfoFromToken(httpClient, apiServer, token)
		uSplit := strings.Split(userpass, ":")
		if len(uSplit) > 1 {
			user["password"] = uSplit[1]
		}
		return user, accessGroups
	}

	if userpass != "" {
		var user, pass string
		uSplit := strings.Split(userpass, ":")
		if len(uSplit) > 1 {
			pass = uSplit[1]
		} else {
			fmt.Print("Password: ")
			pw, err := term.ReadPassword(int(syscall.Stdin))
			if err != nil {
				log.Fatal(err)
			}
			pass = string(pw)
		}
		return authenticator.AuthenticateUser(httpClient, apiServer, user, pass)
	}

	var username string
	fmt.Print("Username: ")
	_, err := fmt.Scan(&username)
	if err != nil {
		fatalExit(err)
	}
	fmt.Print("Password: ")
	pw, err := term.ReadPassword(int(syscall.Stdin))
	if err != nil {
		fatalExit(err)
	}
	return authenticator.AuthenticateUser(httpClient, apiServer, username, string(pw))
}
