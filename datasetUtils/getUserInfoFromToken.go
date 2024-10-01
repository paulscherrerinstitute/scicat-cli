package datasetUtils

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
)

type ReturnedUser struct {
	Id string `json:"id"`
}

type UserIdentity struct {
	Profile Profile `json:"profile"`
}

type Profile struct {
	Username     string   `json:"username"`
	DisplayName  string   `json:"displayName"`
	AccessGroups []string `json:"accessGroups"`
	Emails       []Email  `json:"emails"`
}

type Email struct {
	Value string `json:"value"`
}

func GetUserInfoFromToken(client *http.Client, APIServer string, token string) (map[string]string, []string, error) {
	var newUserInfo ReturnedUser
	var accessGroups []string
	u := map[string]string{}
	bearerToken := fmt.Sprintf("Bearer %s", token)

	// get user info (does not contain access groups) [1st request]
	req1, err := http.NewRequest("GET", APIServer+"/users/my/self", nil)
	if err != nil {
		return map[string]string{}, []string{}, err
	}
	req1.Header.Set("Authorization", bearerToken)
	resp1, err := client.Do(req1)
	if err != nil {
		return map[string]string{}, []string{}, err
	}
	defer resp1.Body.Close()
	body1, err := io.ReadAll(resp1.Body)
	if err := json.Unmarshal(body1, &newUserInfo); err != nil {
		return map[string]string{}, []string{}, err
	}

	// get extra details about user [2nd request]
	var respObj UserIdentity
	if err != nil {
		return map[string]string{}, []string{}, err
	}
	filterString := url.QueryEscape(fmt.Sprintf("{\"where\":{\"userId\":\"%s\"}}", newUserInfo.Id))
	req2, err := http.NewRequest("GET", APIServer+"/useridentities/findOne?filter="+filterString, nil)
	if err != nil {
		return map[string]string{}, []string{}, err
	}
	req2.Header.Set("Authorization", bearerToken)

	resp2, err := client.Do(req2)
	if err != nil {
		return map[string]string{}, []string{}, err
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != 200 {
		return map[string]string{}, []string{}, fmt.Errorf("could not login with token:%v, status %v", token, resp1.StatusCode)
	}
	body2, err := io.ReadAll(resp2.Body)
	if err != nil {
		return map[string]string{}, []string{}, err
	}
	err = json.Unmarshal(body2, &respObj)
	if err != nil {
		return map[string]string{}, []string{}, err
	}

	// return important user informations
	if respObj.Profile.Username == "" {
		return map[string]string{}, []string{}, fmt.Errorf("could not map a user to the token '%v'", token)
	}
	u["username"] = respObj.Profile.Username
	if len(respObj.Profile.Emails) > 0 {
		u["mail"] = respObj.Profile.Emails[0].Value
	}
	u["displayName"] = respObj.Profile.DisplayName
	u["accessToken"] = token
	accessGroups = respObj.Profile.AccessGroups

	return u, accessGroups, nil
}
