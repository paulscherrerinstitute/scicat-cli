package datasetUtils

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
)

type UserInfo struct {
	CurrentUser      string   `json:"currentUser"`
	CurrentUserEmail string   `json:"currentUserEmail"`
	CurrentGroups    []string `json:"currentGroups"`
}

/* GetUserInfoFromToken makes a GET request to the provided APIServer with the provided token to fetch user information.

Parameters: client: An *http.Client object to make the HTTP request. APIServer: A string representing the URL of the API server. token: A string representing the user's access token.

Returns: A map[string]string where the keys are "username", "mail", "displayName", and "accessToken", and the values are the corresponding user information. A slice of strings representing the groups the user is a member of.

If the HTTP request fails or the response status code is not 200, the function will log the error and terminate the program. If the user information cannot be unmarshalled into the UserInfo struct or the user cannot be mapped to the token, the function will log the error and terminate the program. */
func GetUserInfoFromToken(client *http.Client, APIServer string, token string) (map[string]string, []string) {
	u := make(map[string]string)
	accessGroups := make([]string, 0)

	url := APIServer + "/Users/userInfos?access_token=" + token
	req, err := http.NewRequest("GET", url, nil)
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)

	if resp.StatusCode != 200 {
		log.Fatalf("Could not login with token:%v, status %v", token, resp.StatusCode)
	}

	var respObj UserInfo
	err = json.Unmarshal(body, &respObj)
	if err != nil {
		log.Fatal(err)
	}

	if respObj.CurrentUser != "" {
		//log.Printf("Found the following user for this token %v", respObj[0])
		u["username"] = respObj.CurrentUser
		u["mail"] = respObj.CurrentUserEmail
		u["displayName"] = respObj.CurrentUser
		u["accessToken"] = token
		log.Printf("User authenticated: %s %s\n", u["displayName"], u["mail"])
		accessGroups = respObj.CurrentGroups
		log.Printf("User is member in following groups: %v\n", accessGroups)
	} else {
		log.Fatalf("Could not map a user to the token %v", token)
	}
	return u, accessGroups
}
