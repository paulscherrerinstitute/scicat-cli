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

// Authenticate handles user authentication by prompting the user for their credentials,
// validating these credentials against the authentication server,
// and returning an authentication token if the credentials are valid.
// This token can then be used for authenticated requests to the server.
// If the credentials are not valid, the function returns an error.
func authenticate(authenticator datasetUtils.RealAuthenticator, httpClient *http.Client, apiServer string, userpass string, token string) (map[string]string, []string) {
	if token != "" {
		user, accessGroups := authenticator.GetUserInfoFromToken(httpClient, apiServer, token)
		uSplit := strings.Split(userpass, ":")
		if len(uSplit) >= 1 {
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
		log.Fatal(err)
	}
	fmt.Print("Password: ")
	pw, err := term.ReadPassword(int(syscall.Stdin))
	if err != nil {
		log.Fatal(err)
	}
	return authenticator.AuthenticateUser(httpClient, apiServer, username, string(pw))
}
