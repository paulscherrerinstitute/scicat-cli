package datasetIngestor

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/fatih/color"
)

/*
getAVFromPolicy  retrieves the AV (?) from a policy. 

Parameters:
- client: An HTTP client used to send requests.
- APIServer: The URL of the API server.
- user: A map containing user information. It should contain an "accessToken" key.
- owner: The owner of the policy.

The function constructs a URL using the APIServer, owner, and user's access token, and sends a GET request to this URL. 
If the response status code is 200, it reads the response body and unmarshals it into a slice of Policy structs. 
If there are no policies available for the owner, it logs a warning and sets the level to "low". 
If there are policies available, it sets the level to the TapeRedundancy of the first policy.

Returns:
- level: The TapeRedundancy level of the first policy if available, otherwise "low".
*/
func getAVFromPolicy(client *http.Client, APIServer string, user map[string]string, owner string) (level string) {
	var myurl = APIServer + "/Policies?filter=%7B%22where%22%3A%7B%22ownerGroup%22%3A%22" + owner + "%22%7D%7D&access_token=" + user["accessToken"]
	resp, _ := client.Get(myurl)
	defer resp.Body.Close()

	level = "low"
	if resp.StatusCode == 200 {
		body, _ := io.ReadAll(resp.Body)
		type Policy struct {
			TapeRedundancy string
			AutoArchive    bool
		}
		var policies []Policy
		_ = json.Unmarshal(body, &policies)
		if len(policies) == 0 {
			color.Set(color.FgYellow)
			log.Printf("No Policy available for owner %v\n", owner)
			color.Set(color.FgGreen)
		} else {
			level = policies[0].TapeRedundancy
		}
	}
	return level
}

func UpdateMetaData(client *http.Client, APIServer string, user map[string]string,
	originalMap map[string]string, metaDataMap map[string]interface{}, startTime time.Time, endTime time.Time, owner string, tapecopies *int) {
	// add real creationTime if not yet existing
	color.Set(color.FgGreen)
	if metaDataMap["creationTime"] == DUMMY_TIME {
		originalMap["creationTime"] = metaDataMap["creationTime"].(string)
		metaDataMap["creationTime"] = startTime
		log.Printf("creationTime field added: %v\n", metaDataMap["creationTime"])
	}

	if metaDataMap["ownerGroup"] == DUMMY_OWNER {
		originalMap["ownerGroup"] = metaDataMap["ownerGroup"].(string)
		metaDataMap["ownerGroup"] = owner
		log.Printf("ownerGroup field added from file info: %v\n", metaDataMap["ownerGroup"])
	}

	if metaDataMap["type"] == "raw" {
		if metaDataMap["endTime"] == DUMMY_TIME {
			originalMap["endTime"] = metaDataMap["endTime"].(string)
			metaDataMap["endTime"] = endTime
			log.Printf("endTime field added: %v\n", metaDataMap["endTime"])
		}
	}

	// check other fields to be added

	if _, ok := metaDataMap["license"]; !ok {
		metaDataMap["license"] = "CC BY-SA 4.0"
		log.Printf("license field added: %v\n", metaDataMap["license"])
	}
	if _, ok := metaDataMap["isPublished"]; !ok {
		metaDataMap["isPublished"] = false
		log.Printf("isPublished field added: %v\n", metaDataMap["isPublished"])
	}

	/* update classification depending on policy settings and tapecopies parameter
	   precedence order from highest to lowest:
	     - tapecopies parameter for AV setting
		 - classification defined in metadata.json
		 - policy setting (give warning if this is different from value calculated so far)

	*/
	if _, ok := metaDataMap["classification"]; !ok {
		// take default from policy settings
		metaDataMap["classification"] = "IN=medium" + ",AV=" + getAVFromPolicy(client, APIServer, user, metaDataMap["ownerGroup"].(string)) + ",CO=low"
		log.Printf("classification field added: %v\n", metaDataMap["classification"])
	}

	if *tapecopies == 1 || *tapecopies == 2 {
		var av string
		if *tapecopies == 1 {
			av = "AV=low"
		}
		if *tapecopies == 2 {
			av = "AV=medium"
		}
		var newresult []string
		if val, ok := metaDataMap["classification"]; ok {
			result := strings.Split(val.(string), ",")
			// check for AV field and if existing override
			found := false
			for _, element := range result {
				if element == "" {
					continue
				}
				if strings.HasPrefix(element, "AV=") {
					newresult = append(newresult, av)
					found = true
				} else {
					newresult = append(newresult, element)
				}
			}
			if !found {
				newresult = append(newresult, av)
			}
			metaDataMap["classification"] = strings.Join(newresult, ",")
		} else {
			metaDataMap["classification"] = "IN=medium," + av + ",CO=low"
		}
		log.Printf("classification field adjusted: %s\n", metaDataMap["classification"])
	}

	if strings.Contains(metaDataMap["classification"].(string), "AV=medium") {
		color.Set(color.FgYellow)
		log.Printf("Note: this dataset, if archived, will be copied to two tape copies")
	}
	color.Unset()
}
