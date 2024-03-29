package datasetUtils

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
)

func GetProposal(client *http.Client, APIServer string, ownerGroup string, user map[string]string,
	accessGroups []string) (proposal map[string]interface{}) {

	// Check if ownerGroup is in accessGroups list. No longer needed, done on server side and
	//  takes also accessGroup for beamline accounts into account

	// if user["displayName"] != "ingestor" {
	// 	validOwner := false
	// 	for _, b := range accessGroups {
	// 		if b == ownerGroup {
	// 			validOwner = true
	// 			break
	// 		}
	// 	}
	// 	if validOwner {
	// 		log.Printf("OwnerGroup information %s verified successfully.\n", ownerGroup)
	// 	} else {
	// 		log.Fatalf("You are not member of the ownerGroup %s which is needed to access this data", ownerGroup)
	// 	}
	// }

	filter := `{"where":{"ownerGroup":"` + ownerGroup + `"}}`
	url := APIServer + "/Proposals?access_token=" + user["accessToken"]

	// fmt.Printf("=== resulting filter:%s\n", filter)
	req, err := http.NewRequest("GET", url, nil)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("filter", filter)

	resp, err := client.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)

	// fmt.Printf("response Object:\n%v\n", string(body))

	var respObj []map[string]interface{}
	err = json.Unmarshal(body, &respObj)
	if err != nil {
		log.Fatal(err)
	}
	// the first element contains the actual map
	respMap := make(map[string]interface{})
	if len(respObj) > 0 {
		respMap = respObj[0]
	}
	return respMap
}
