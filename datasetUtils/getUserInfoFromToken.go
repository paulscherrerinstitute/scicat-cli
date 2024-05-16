package datasetUtils

import (
	"encoding/json"
	"log"	
	"fmt"
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
func GetUserInfoFromToken(client *http.Client, APIServer string, token string) (map[string]string, []string, error) {
	u := make(map[string]string)
	accessGroups := make([]string, 0)
	
	// url := APIServer + "/Users/userInfos" # uncomment if you use the Authorization header method
	url := APIServer + "/Users/userInfos?access_token=" + token
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create request: %w", err)
	}
	
	req.Header.Set("Content-Type", "application/json")
	// req.Header.Set("Authorization", "Bearer "+token)  # NOTE: this is a more secure method, but I am not sure whether the server accepts tokens in the Authorization header.
	
	resp, err := client.Do(req)
	if err != nil {
		return nil, nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()
	
	if resp.StatusCode != http.StatusOK {
		return nil, nil, fmt.Errorf("server responded with status code %d", resp.StatusCode)
	}
	
	var respObj UserInfo
	if err := json.NewDecoder(resp.Body).Decode(&respObj); err != nil {
		return nil, nil, fmt.Errorf("failed to decode response: %w", err)
	}
	
	if respObj.CurrentUser == "" {
		return nil, nil, fmt.Errorf("could not map a user to the token %s", token)
	}
	
	u["username"] = respObj.CurrentUser
	u["mail"] = respObj.CurrentUserEmail
	u["displayName"] = respObj.CurrentUser
	u["accessToken"] = token
	log.Printf("User authenticated: %s %s\n", u["displayName"], u["mail"])
	accessGroups = respObj.CurrentGroups
	log.Printf("User is member in following groups: %v\n", accessGroups)
	
	return u, accessGroups, nil
}
