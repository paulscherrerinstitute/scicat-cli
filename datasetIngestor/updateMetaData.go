package datasetIngestor

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"
)

const (
	Classification = "classification"
	AVLow          = "AV=low"
	AVMedium       = "AV=medium"
	INMedium       = "IN=medium"
	COLow          = "CO=low"
)

/*
getAVFromPolicy retrieves the AV (?) from a policy.

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
	level = "low" // default value

	var myurl = APIServer + "/Policies?filter=%7B%22where%22%3A%7B%22ownerGroup%22%3A%22" + owner + "%22%7D%7D&access_token=" + user["accessToken"]
	resp, err := client.Get(myurl)
	if err != nil {
		return level
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return level
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return level
	}
	type Policy struct {
		TapeRedundancy string
		AutoArchive    bool
	}
	var policies []Policy
	_ = json.Unmarshal(body, &policies)
	if len(policies) > 0 {
		level = policies[0].TapeRedundancy
	}
	return level
}

/*
UpdateMetaData updates the metadata of a dataset.

Parameters:
- client: An HTTP client used to send requests.
- APIServer: The URL of the API server.
- user: A map containing user information. It should contain an "accessToken" key.
- originalMap: A map containing the original metadata.
- metaDataMap: A map containing the metadata to be updated.
- startTime: The start time of the dataset.
- endTime: The end time of the dataset.
- owner: The owner of the dataset.
- tapecopies: A pointer to an integer indicating the number of tape copies.

The function updates the following fields in metaDataMap:
- "creationTime": If it's equal to DUMMY_TIME, it's replaced with startTime.
- "ownerGroup": If it's equal to DUMMY_OWNER, it's replaced with owner.
- "endTime": If the "type" field is "raw" and "endTime" is equal to DUMMY_TIME, it's replaced with endTime.
- "license": If it doesn't exist, it's set to "CC BY-SA 4.0".
- "isPublished": If it doesn't exist, it's set to false.
- "classification": If it doesn't exist, it's set to "IN=medium,AV=<AV from policy>,CO=low". If tapecopies is 1 or 2, the "AV" part is replaced with "AV=low" or "AV=medium" respectively.

The function logs a message each time it updates a field. If the "classification" field contains "AV=medium", it also logs a note that the dataset will be copied to two tape copies.

The function does not return a value.
*/
func UpdateMetaData(client *http.Client, APIServer string, user map[string]string,
	originalMap map[string]string, metaDataMap map[string]interface{}, startTime time.Time, endTime time.Time, owner string, tapecopies int) {
	updateFieldIfDummy(metaDataMap, originalMap, "creationTime", DUMMY_TIME, startTime)
	updateFieldIfDummy(metaDataMap, originalMap, "ownerGroup", DUMMY_OWNER, owner)

	if metaDataMap["type"] == "raw" {
		updateFieldIfDummy(metaDataMap, originalMap, "endTime", DUMMY_TIME, endTime)
	}

	addFieldIfNotExists(metaDataMap, "license", "CC BY-SA 4.0")
	addFieldIfNotExists(metaDataMap, "isPublished", false)

	updateClassificationField(client, APIServer, user, metaDataMap, tapecopies)
}

func updateFieldIfDummy(metaDataMap map[string]interface{}, originalMap map[string]string, fieldName string, dummyValue interface{}, newValue interface{}) {
	if metaDataMap[fieldName] == dummyValue {
		originalMap[fieldName] = metaDataMap[fieldName].(string)
		metaDataMap[fieldName] = newValue
	}
}

func addFieldIfNotExists(metaDataMap map[string]interface{}, fieldName string, value interface{}) {
	if _, ok := metaDataMap[fieldName]; !ok {
		metaDataMap[fieldName] = value
	}
}

func updateClassificationField(client *http.Client, APIServer string, user map[string]string, metaDataMap map[string]interface{}, tapecopies int) {
	if _, ok := metaDataMap[Classification]; !ok {
		addDefaultClassification(client, APIServer, user, metaDataMap)
	}

	if tapecopies == 1 || tapecopies == 2 {
		updateAVField(metaDataMap, tapecopies)
	}
}

func addDefaultClassification(client *http.Client, APIServer string, user map[string]string, metaDataMap map[string]interface{}) {
	metaDataMap[Classification] = INMedium + ",AV=" + getAVFromPolicy(client, APIServer, user, metaDataMap["ownerGroup"].(string)) + "," + COLow
}

func updateAVField(metaDataMap map[string]interface{}, tapecopies int) {
	av := getAVValue(tapecopies)
	if _, ok := metaDataMap[Classification]; ok {
		newresult := getUpdatedClassification(metaDataMap, av)
		metaDataMap[Classification] = strings.Join(newresult, ",")
	} else {
		metaDataMap[Classification] = INMedium + "," + av + "," + COLow
	}
}

func getAVValue(tapecopies int) string {
	if tapecopies == 1 {
		return AVLow
	}
	return AVMedium
}

func getUpdatedClassification(metaDataMap map[string]interface{}, av string) []string {
	result := strings.Split(metaDataMap[Classification].(string), ",")
	// check for AV field and if existing override it
	newresult := make([]string, 0, len(result))
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
	return newresult
}
