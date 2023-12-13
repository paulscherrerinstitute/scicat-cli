package datasetUtils

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
)

type UserInfo struct {
	CurrentUser      string   `json:"currentUser"`
	CurrentUserEmail string   `json:"currentUserEmail"`
	CurrentGroups    []string `json:"currentGroups"`
}

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
	body, err := ioutil.ReadAll(resp.Body)

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
		log.Fatalln("Could not map a user to the token %v", token)
	}
	return u, accessGroups

}
