package datasetUtils

import (
	"bytes"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"strings"
)

func AuthenticateUser(client *http.Client, APIServer string, username string, password string) (map[string]string, []string) {
	u := make(map[string]string)
	accessGroups := make([]string, 0)

	var credential = make(map[string]string)

	credential["username"] = username
	credential["password"] = password
	cred, _ := json.Marshal(credential)

	mail := ""
	displayName := ""
	accessToken := ""

	// try functional accounts first

	req, err := http.NewRequest("POST", APIServer+"/Users/login", bytes.NewBuffer(cred))
	if err != nil {
		log.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == 200 {
		// important: use capital first character in field names!
		type Auth struct {
			UserId string
			Id     string
		}
		decoder := json.NewDecoder(resp.Body)
		var auth Auth
		err := decoder.Decode(&auth)
		if err != nil {
			log.Fatal(err)
		}
		// now get email from User collections
		type User struct {
			Username string
			Email    string
		}
		user := new(User)
		var myurl = APIServer + "/Users/" + auth.UserId + "?access_token=" + auth.Id
		//fmt.Println("Url:", myurl)
		GetJson(client, myurl, user)
		mail = user.Email
		displayName = user.Username
		accessToken = auth.Id
	} else {
		// then try normal user account
		req, err = http.NewRequest("POST", strings.Replace(APIServer, "api/v3", "auth/msad", 1), bytes.NewBuffer(cred))
		if err != nil {
			log.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err = client.Do(req)
		if err != nil {
			log.Fatal(err)
		}
		defer resp.Body.Close()
		if resp.StatusCode == 200 {
			// response Body: {"access_token":"9EeA7sNeJrzAHpltZi8yVWMntgEPEjikmzFns7GXgtd00GYNEezZUO4at4q5MDIz","userId":"5971bfd88051720800aafc51"}
			// important: use capital first character in field names!
			type Auth2 struct {
				UserId       string
				Access_token string
			}
			decoder := json.NewDecoder(resp.Body)
			var auth2 Auth2
			err := decoder.Decode(&auth2)
			if err != nil {
				log.Fatal(err)
			}
			// now get email from UserIdentity collections
			var myurl = APIServer + "/UserIdentities?filter=%7B%22where%22%3A%7B%22userId%22%3A%22" + auth2.UserId + "%22%7D%7D&access_token=" + auth2.Access_token
			resp, err := client.Get(myurl)
			if err != nil {
				log.Fatal(err)
			}
			defer resp.Body.Close()
			body, _ := io.ReadAll(resp.Body)
			//fmt.Printf("Result:%s",string(body))
			type NormalUser struct {
				Profile struct {
					DisplayName  string
					Email        string
					AccessGroups []string
				}
			}
			var users []NormalUser
			_ = json.Unmarshal(body, &users)
			mail = users[0].Profile.Email
			displayName = users[0].Profile.DisplayName
			accessGroups = users[0].Profile.AccessGroups
			accessToken = auth2.Access_token

			// create Kerberos TGT for normal user account, if not yet existing
			RunKinit(username, password)
		}
	}

	if resp.StatusCode != 200 {
		log.Fatalf("User %s: authentication failed", username)
	}

	u["username"] = username
	u["mail"] = mail
	u["displayName"] = displayName
	u["accessToken"] = accessToken
	u["password"] = password
	log.Printf("User authenticated: %s %s\n", displayName, mail)
	log.Printf("User is member in following a or p groups: %v\n", accessGroups)
	return u, accessGroups
}
