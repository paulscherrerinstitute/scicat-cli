package cliutils

import (
	"fmt"
	"log"
	"net/http"
	"strings"
	"syscall"

	"golang.org/x/term"
	"github.com/paulscherrerinstitute/scicat-cli/v3/datasetUtils"
)

// Authenticator is an abstraction used to support testing and custom auth backends.
type Authenticator interface {
	AuthenticateUser(httpClient *http.Client, APIServer string, username string, password string) (map[string]string, []string, error)
	GetUserInfoFromToken(httpClient *http.Client, APIServer string, token string) (map[string]string, []string, error)
}

// RealAuthenticator delegates to real datasetUtils auth endpoints.
type RealAuthenticator struct{}

func (r RealAuthenticator) AuthenticateUser(httpClient *http.Client, APIServer string, username string, password string) (map[string]string, []string, error) {
	user, groups, err := datasetUtils.AuthenticateUser(httpClient, APIServer, username, password, false)
	if err != nil {
		user, groups, err = datasetUtils.AuthenticateUser(httpClient, APIServer, username, password, true)
		if err != nil {
			return map[string]string{}, []string{}, err
		}
		datasetUtils.RunKinit(username, password) // PSI specific Kerberos user creation
	}
	return user, groups, err
}

func (r RealAuthenticator) GetUserInfoFromToken(httpClient *http.Client, APIServer string, token string) (map[string]string, []string, error) {
	return datasetUtils.GetUserInfoFromToken(httpClient, APIServer, token)
}

var oidcTokenProvider func(string) string

// SetOIDCTokenProvider configures the function used to fetch OIDC tokens.
func SetOIDCTokenProvider(provider func(string) string) {
	oidcTokenProvider = provider
}

// Authenticate handles user authentication by prompting for credentials as needed.
func Authenticate(authenticator Authenticator, httpClient *http.Client, apiServer string, userpass string, token string, oidc bool, overrideFatalExit ...func(v ...any)) (map[string]string, []string, error) {
	fatalExit := log.Fatal // by default, call log fatal
	if len(overrideFatalExit) == 1 {
		fatalExit = overrideFatalExit[0]
	}

	if oidc {
		if oidcTokenProvider == nil {
			return map[string]string{}, []string{}, fmt.Errorf("oidc token provider is not configured")
		}
		token = oidcTokenProvider(apiServer + "/auth/oidc?client=CLI")
		user, accessGroups, err := authenticator.GetUserInfoFromToken(httpClient, apiServer, token)
		if err != nil {
			return map[string]string{}, []string{}, err
		}
		return user, accessGroups, nil
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
