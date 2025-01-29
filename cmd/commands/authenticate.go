package cmd

import (
	"fmt"
	"log"
	"net/http"
	"strings"
	"syscall"

	"github.com/paulscherrerinstitute/scicat-cli/v3/datasetUtils"
	"golang.org/x/term"
)

// An interface with the methods so that we can mock them in tests
type Authenticator interface {
	AuthenticateUser(httpClient *http.Client, APIServer string, username string, password string) (map[string]string, []string, error)
	GetUserInfoFromToken(httpClient *http.Client, APIServer string, token string) (map[string]string, []string, error)
}

type RealAuthenticator struct{}

func (r RealAuthenticator) AuthenticateUser(httpClient *http.Client, APIServer string, username string, password string) (map[string]string, []string, error) {
	user, groups, err := datasetUtils.AuthenticateUser(httpClient, APIServer, username, password, false)
	if err != nil {
		user, groups, err = datasetUtils.AuthenticateUser(httpClient, APIServer, username, password, true)
		if err != nil {
			return map[string]string{}, []string{}, err
		}
		datasetUtils.RunKinit(username, password) // PSI specific KerberOS user creation
	}
	return user, groups, err
}

func (r RealAuthenticator) GetUserInfoFromToken(httpClient *http.Client, APIServer string, token string) (map[string]string, []string, error) {
	return datasetUtils.GetUserInfoFromToken(httpClient, APIServer, token)
}

// Authenticate handles user authentication by prompting the user for their credentials,
// validating these credentials against the authentication server,
// and returning an authentication token if the credentials are valid.
// This token can then be used for authenticated requests to the server.
// If the credentials are not valid, the function returns an error.
func authenticate(authenticator Authenticator, httpClient *http.Client, apiServer string, userpass string, token string, overrideFatalExit ...func(v ...any)) (map[string]string, []string, error) {
	fatalExit := log.Fatal // by default, call log fatal
	if len(overrideFatalExit) == 1 {
		fatalExit = overrideFatalExit[0]
	}
	if token != "" {
		user, accessGroups, err := authenticator.GetUserInfoFromToken(httpClient, apiServer, token)
		if err != nil {
			return map[string]string{}, []string{}, err
		}
		uSplit := strings.Split(userpass, ":")
		if len(uSplit) > 1 {
			user["password"] = uSplit[1]
		}
		return user, accessGroups, nil
	}

	if userpass != "" {
		var user, pass string
		uSplit := strings.Split(userpass, ":")
		if len(uSplit) > 1 {
			user = uSplit[0]
			pass = uSplit[1]
		} else {
			user = userpass
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
