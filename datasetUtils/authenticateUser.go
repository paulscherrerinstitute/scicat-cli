package datasetUtils

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

type loginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type loginResponse struct {
	AccessToken string `json:"access_token"`
}

type identityResponse struct {
	Profile struct {
		Username     string   `json:"username"`
		DisplayName  string   `json:"displayName"`
		Email        string   `json:"email"`
		AccessGroups []string `json:"accessGroups"`
	} `json:"profile"`
}

func newLoginRequestJson(username string, password string) ([]byte, error) {
	l := loginRequest{
		Username: username,
		Password: password,
	}
	return json.Marshal(l)
}

func AuthenticateUser(client *http.Client, APIServer string, username string, password string, ldapLogin bool) (map[string]string, []string, error) {
	loginReqJson, err := newLoginRequestJson(username, password)
	if err != nil {
		return map[string]string{}, []string{}, err
	}

	reqUrl := APIServer + "/auth/login" // "local" user login
	if ldapLogin {
		reqUrl = APIServer + "/auth/ldap" // "normal" user login
	}
	req, err := http.NewRequest("POST", reqUrl, bytes.NewBuffer(loginReqJson))
	if err != nil {
		return map[string]string{}, []string{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return map[string]string{}, []string{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 201 {
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return map[string]string{}, []string{}, fmt.Errorf("error when logging in: unknown error (can't parse body)")
		}
		return map[string]string{}, []string{}, fmt.Errorf("error when logging in: '%s'", string(body))
	}

	respJson, err := io.ReadAll(resp.Body)
	if err != nil {
		return map[string]string{}, []string{}, err
	}

	var lr loginResponse
	err = json.Unmarshal(respJson, &lr)
	if err != nil {
		return map[string]string{}, []string{}, err
	}

	req, err = http.NewRequest("GET", APIServer+"/users/my/identity", nil)
	if err != nil {
		return map[string]string{}, []string{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+lr.AccessToken)

	resp, err = client.Do(req)
	if err != nil {
		return map[string]string{}, []string{}, err
	}
	defer resp.Body.Close()

	respJson, err = io.ReadAll(resp.Body)
	if err != nil {
		return map[string]string{}, []string{}, err
	}

	var ir identityResponse
	err = json.Unmarshal(respJson, &ir)
	if err != nil {
		return map[string]string{}, []string{}, err
	}

	u := make(map[string]string)
	u["username"] = ir.Profile.Username
	u["mail"] = ir.Profile.Email
	u["displayName"] = ir.Profile.DisplayName
	u["accessToken"] = lr.AccessToken
	u["password"] = password
	return u, ir.Profile.AccessGroups, nil
}
