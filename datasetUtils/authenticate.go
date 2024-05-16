package datasetUtils

import (
	"golang.org/x/crypto/ssh/terminal"
	"log"
	"net/http"
	"strings"
	"syscall"
)

func Authenticate(httpClient *http.Client, APIServer string, token *string, userpass *string) (map[string]string, []string) {
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
		user, accessGroups = AuthenticateUser(httpClient, APIServer, username, password)
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
