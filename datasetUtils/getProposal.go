package datasetUtils

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
)

/*
GetProposal retrieves a proposal from a given API server.

Parameters:
- client: An *http.Client object used to send the request.
- APIServer: A string representing the base URL of the API server.
- ownerGroup: A string representing the owner group of the proposal.
- user: A map containing user information, including an access token.

The function constructs a filter based on the ownerGroup, then sends a GET request to the API server with the filter and user's access token. The response is then parsed into a map and returned.

If the request or JSON unmarshalling fails, the function will log the error and terminate the program.

Returns:
- A map representing the proposal. If no proposal is found, an empty map is returned.
*/
func GetProposal(client *http.Client, APIServer string, ownerGroup string, user map[string]string) (map[string]interface{}, error) {
	filter := fmt.Sprintf(`{"where":{"ownerGroup":"%s"}}`, ownerGroup)
	url := fmt.Sprintf("%s/proposals?filters=%s", APIServer, url.QueryEscape(filter))

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+user["accessToken"])

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("failing status code: %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var respObj []map[string]interface{}
	err = json.Unmarshal(body, &respObj)
	if err != nil {
		return nil, err
	}

	respMap := make(map[string]interface{})
	if len(respObj) > 0 {
		respMap = respObj[0]
	}

	return respMap, nil
}
