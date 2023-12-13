package datasetUtils

import (
    "net/http"
    "encoding/json"
)



func GetJson(client *http.Client, myurl string, target interface{}) error {
	r, err := client.Get(myurl)
	if err != nil {
		return err
	}
	defer r.Body.Close()

	return json.NewDecoder(r.Body).Decode(target)
}
