package datasetUtils

import (
	"golang.org/x/crypto/ssh/terminal"
	"log"
	"net/http"
	"strings"
	"syscall"
)

// Authenticate handles user authentication by prompting the user for their credentials,
// validating these credentials against the authentication server, 
// and returning an authentication token if the credentials are valid. 
// This token can then be used for authenticated requests to the server.
// If the credentials are not valid, the function returns an error.
func Authenticate(auth Authenticator, httpClient *http.Client, APIServer string, token *string, userpass *string) (map[string]string, []string) {
	user := make(map[string]string)
	accessGroups := make([]string, 0)

	// if token is defined do not ask for username/password interactively

	if *token == "" {
		username := ""
		password := ""
		if *userpass == "" {
			log.Printf("Your username: ")
			scanner.Scan()
			username = scanner.Text()

			log.Printf("Your password: ")
			bytePassword, _ := terminal.ReadPassword(int(syscall.Stdin))
			password = string(bytePassword)
			//log.Println() // it's necessary to add a new line after user's input
		} else {
			u := strings.Split(*userpass, ":")
			if len(u) == 2 {
				password = strings.Split(*userpass, ":")[1]
			} else {
				log.Printf("Your password: ")
				bytePassword, _ := terminal.ReadPassword(int(syscall.Stdin))
				password = string(bytePassword)
			}
			username = strings.Split(*userpass, ":")[0]
		}
		user, accessGroups = auth.AuthenticateUser(httpClient, APIServer, username, password)
	} else {
		var err error
		user, accessGroups, err = GetUserInfoFromToken(httpClient, APIServer, *token)
		if err != nil {
			log.Fatalf("Failed to get user info from token: %v", err)
		}

		// extract password if defined in userpass value
		u := strings.Split(*userpass, ":")
		if len(u) == 2 {
			user["password"] = strings.Split(*userpass, ":")[1]
		}
	}
	return user, accessGroups
}

// An interface with the methods so that we can mock them in tests
type Authenticator interface {
	AuthenticateUser(httpClient *http.Client, APIServer string, username string, password string) (map[string]string, []string)
	GetUserInfoFromToken(httpClient *http.Client, APIServer string, token string) (map[string]string, []string)
}

type RealAuthenticator struct{}

func (r *RealAuthenticator) AuthenticateUser(httpClient *http.Client, APIServer string, username string, password string) (map[string]string, []string) {
    return AuthenticateUser(httpClient, APIServer, username, password)
}

func (r *RealAuthenticator) GetUserInfoFromToken(httpClient *http.Client, APIServer string, token string) (map[string]string, []string) {
    return GetUserInfoFromToken(httpClient, APIServer, token)
}
