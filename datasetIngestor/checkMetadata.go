package datasetIngestor

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/fatih/color"
	"github.com/paulscherrerinstitute/scicat-cli/v3/datasetUtils"
)

const (
	ErrIllegalKeys = "metadata contains keys with illegal characters (., [], $, or <>)"
	DUMMY_TIME     = "2300-01-01T11:11:11.000Z"
	DUMMY_OWNER    = "x12345"
)

const unknown = "unknown"
const raw = "raw"

// a combined function that reads and checks metadata, gathers missing metadata and returns the metadata map, source folder and beamline account check
func ReadAndCheckMetadata(client *http.Client, APIServer string, metadatafile string, user map[string]string, accessGroups []string) (metaDataMap map[string]interface{}, sourceFolder string, beamlineAccount bool, err error) {
	metaDataMap, err = ReadMetadataFromFile(metadatafile)
	if err != nil {
		return nil, "", false, err
	}
	sourceFolder, beamlineAccount, err = CheckMetadata(client, APIServer, metaDataMap, user, accessGroups)
	return metaDataMap, sourceFolder, beamlineAccount, err
}

func CheckMetadata(client *http.Client, APIServer string, metaDataMap map[string]interface{}, user map[string]string, accessGroups []string) (sourceFolder string, beamlineAccount bool, err error) {
	if keys := CollectIllegalKeys(metaDataMap); len(keys) > 0 {
		return "", false, errors.New(ErrIllegalKeys + ": \"" + strings.Join(keys, "\", \"") + "\"")
	}

	beamlineAccount, err = CheckUserAndOwnerGroup(user, accessGroups, metaDataMap)
	if err != nil {
		return "", false, err
	}

	err = GatherMissingMetadata(user, metaDataMap, client, APIServer, accessGroups)
	if err != nil {
		return "", false, err
	}

	err = CheckMetadataValidity(client, APIServer, user["accessToken"], metaDataMap)
	if err != nil {
		return "", false, err
	}

	sourceFolder, err = GetSourceFolder(metaDataMap)
	if err != nil {
		return "", false, err
	}

	return sourceFolder, beamlineAccount, nil
}

// ReadMetadataFromFile reads the metadata from the file and unmarshals it into a map.
func ReadMetadataFromFile(metadatafile string) (map[string]interface{}, error) {
	b, err := os.ReadFile(metadatafile) // just pass the file name
	if err != nil {
		return nil, err
	}
	// Unmarshal the JSON metadata into an interface{} object.
	var metadataObj interface{} // Using interface{} allows metadataObj to hold any type of data, since it has no defined methods.
	err = json.Unmarshal(b, &metadataObj)
	if err != nil {
		return nil, err
	}

	// Use type assertion to convert the interface{} object to a map[string]interface{}.
	metaDataMap := metadataObj.(map[string]interface{}) // `.(` is type assertion: a way to extract the underlying value of an interface and check whether it's of a specific type.
	return metaDataMap, err
}

// collects keys with illegal characters
func CollectIllegalKeys(metadata map[string]interface{}) []string {
	stack := []map[string]interface{}{metadata}
	keys := []string{}

	for len(stack) > 0 {
		item := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		for key, value := range item {
			if keyContainsIllegalCharacters(key) {
				keys = append(keys, key)
			}

			switch v := value.(type) {
			case map[string]interface{}:
				stack = append(stack, v)
			case []interface{}:
				for _, vItem := range v {
					// convert this to switch if other types are needed
					if vMap, ok := vItem.(map[string]interface{}); ok {
						stack = append(stack, vMap)
					}
				}
			}
		}
	}

	return keys
}

func keyContainsIllegalCharacters(s string) bool {
	// Check if the string contains periods, brackets, or other illegal characters
	// You can adjust this condition based on your specific requirements
	for _, char := range s {
		if char == '.' || char == '[' || char == ']' || char == '<' || char == '>' || char == '$' {
			return true
		}
	}
	return false
}

// CheckUserAndOwnerGroup checks the user and owner group and returns whether the user is a beamline account.
func CheckUserAndOwnerGroup(user map[string]string, accessGroups []string, metaDataMap map[string]interface{}) (bool, error) {
	if user["displayName"] == "ingestor" {
		return false, nil
	}

	// Check if the metadata contains the "ownerGroup" key.
	ownerGroup, ok := metaDataMap["ownerGroup"]
	if !ok {
		// NOTE: so if there's no ownergroup, we can pass this check?
		return false, fmt.Errorf("no OwnerGroup attribute present in metadata")
	}

	// Iterate over accessGroups to validate the owner group.
	for _, b := range accessGroups {
		if b == ownerGroup {
			//log.Printf("OwnerGroup information %s verified successfully.\n", ownerGroup)
			return false, nil
		}
	}

	// NOTE: beamline accounts seem to be very PSI specific.
	// If the owner group is not valid, check for beamline-specific accounts.
	if creationLocation, ok := metaDataMap["creationLocation"]; ok {
		// Split the creationLocation string to extract beamline-specific information.
		parts := strings.Split(creationLocation.(string), "/")
		expectedAccount := ""
		if len(parts) == 4 {
			expectedAccount = strings.ToLower(parts[2]) + strings.ToLower(parts[3])
		}
		// If the user matches the expected beamline account, grant ingest access.
		if user["displayName"] == expectedAccount {
			//log.Printf("Beamline specific dataset %s - ingest granted.\n", expectedAccount)
			return true, nil
		} else {
			return false, fmt.Errorf("you are neither member of the ownerGroup %s nor the needed beamline account %s", ownerGroup, expectedAccount)
		}
	} else {
		// for other data just check user name
		// this is a quick and dirty test. Should be replaced by test for "globalaccess" role. TODO
		// facilities: ["SLS", "SINQ", "SWISSFEL", "SmuS"],
		u := user["displayName"]
		if strings.HasPrefix(u, "sls") ||
			strings.HasPrefix(u, "swissfel") ||
			strings.HasPrefix(u, "sinq") ||
			strings.HasPrefix(u, "smus") {
			return true, nil
		}
	}

	// NOTE: we can get to this part after the last else,
	//   this lacks an error state for not being a beamline account or part of an expected owner group?
	return false, nil
}

func isValidDomain(domain string) bool {
	// Regular expression to validate domain name
	regex := `^(?:a-zA-Z0-9?\.)+[a-zA-Z]{2,}$`
	match, _ := regexp.MatchString(regex, domain)
	return match
}

// getHost is a function that attempts to retrieve and return the fully qualified domain name (FQDN) of the current host.
// If it encounters any error during the process, it falls back to returning "unknown", as a simple hostname won't work with v4 backend
func getHost() string {
	// Try to get the hostname of the current machine.
	hostname, err := os.Hostname()
	if err != nil {
		return unknown
	}

	addrs, err := net.LookupIP(hostname)
	if err != nil {
		return unknown
	}

	for _, addr := range addrs {
		ipv4 := addr.To4()
		if ipv4 == nil {
			continue
		}
		ip, err := ipv4.MarshalText()
		if err != nil {
			continue
		}
		hosts, err := net.LookupAddr(string(ip))
		if err != nil || len(hosts) == 0 {
			continue
		}
		fqdn := strings.TrimSuffix(hosts[0], ".") // fqdn without trailing dot
		if !isValidDomain(fqdn) {
			continue
		}
		return fqdn
	}
	return unknown
}

// GatherMissingMetadata augments missing metadata fields.
func GatherMissingMetadata(user map[string]string, metaDataMap map[string]interface{}, client *http.Client, APIServer string, accessGroups []string) error {
	color.Set(color.FgGreen)
	defer color.Unset()

	// optionally gather missing owner metadata
	if _, ok := metaDataMap["owner"]; !ok {
		metaDataMap["owner"] = user["displayName"]
		//log.Printf("owner field added: %s", metaDataMap["owner"])
	}
	if _, ok := metaDataMap["ownerEmail"]; !ok {
		metaDataMap["ownerEmail"] = user["mail"]
		//log.Printf("ownerEmail field added: %s", metaDataMap["ownerEmail"])
	}
	if _, ok := metaDataMap["contactEmail"]; !ok {
		metaDataMap["contactEmail"] = user["mail"]
		//log.Printf("contactEmail field added: %s", metaDataMap["contactEmail"])
	}

	// and sourceFolderHost
	if _, ok := metaDataMap["sourceFolderHost"]; !ok {
		hostname := getHost()
		if hostname != unknown {
			metaDataMap["sourceFolderHost"] = hostname
			//log.Printf("sourceFolderHost field added: %s", metaDataMap["sourceFolderHost"])
		}
	}

	// for raw data add PI if missing
	if err := addPrincipalInvestigatorFromProposal(user, metaDataMap, client, APIServer); err != nil {
		return err
	}

	// add/append accessGroups entry for beamline if creationLocation is defined
	if value, exists := metaDataMap["creationLocation"]; exists {
		var parts = strings.Split(value.(string), "/")
		if len(parts) == 4 {
			newGroup := strings.ToLower(parts[2]) + strings.ToLower(parts[3])
			if accessGroups, ok := metaDataMap["accessGroups"]; ok {
				switch v := accessGroups.(type) {
				case []string:
					metaDataMap["accessGroups"] = append(v, newGroup)
				default:
					return fmt.Errorf("'accessGroups' is not a list of strings")
				}
			} else {
				metaDataMap["accessGroups"] = []string{newGroup}
			}
		}
	}

	return nil
}

func addPrincipalInvestigatorFromProposal(user map[string]string, metaDataMap map[string]interface{}, client *http.Client, APIServer string) error {
	typeVal, ok := metaDataMap["type"]
	if !ok {
		return fmt.Errorf("type doesn't exist as an attribute")
	}
	dstype, ok := typeVal.(string)
	if !ok {
		return fmt.Errorf("type is not a string")
	}

	if dstype != raw {
		return nil // return if not raw
	}
	if _, ok := metaDataMap["principalInvestigator"]; ok {
		return nil // return if present
	}

	val, ok := metaDataMap["ownerGroup"]
	if !ok {
		return fmt.Errorf("ownerGroup is not present in metadata attributes")
	}

	ownerGroup, ok := val.(string)
	if !ok {
		return fmt.Errorf("ownerGroup is not a string")
	}

	proposal, err := datasetUtils.GetProposal(client, APIServer, ownerGroup, user)
	if err != nil {
		return fmt.Errorf("failed to get proposal: %v", err)
	}

	if val, ok := proposal["pi_email"]; ok {
		piEmail, ok := val.(string)
		if !ok {
			return fmt.Errorf("'pi_email' field in proposal is not a string")
		}
		metaDataMap["principalInvestigator"] = piEmail
		//log.Printf("principalInvestigator field added: %s", metaDataMap["principalInvestigator"])
	} else {
		return errors.New("'pi_email' field is missing from proposal")
	}
	return nil
}

// CheckMetadataValidity checks the validity of the metadata by calling the appropriate API.
func CheckMetadataValidity(client *http.Client, APIServer string, token string, metaDataMap map[string]interface{}) error {
	// add dummy data for fields which can only be filled after file scan to pass the validity test
	if _, exists := metaDataMap["ownerGroup"]; !exists {
		metaDataMap["ownerGroup"] = DUMMY_OWNER
	}
	if _, exists := metaDataMap["creationTime"]; !exists {
		metaDataMap["creationTime"] = DUMMY_TIME
	}
	if metaDataMap["type"] == raw {
		if _, exists := metaDataMap["endTime"]; !exists {
			metaDataMap["endTime"] = DUMMY_TIME
		}
	}

	// request validity check (must be logged-in)
	bmm, err := json.Marshal(metaDataMap)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", APIServer+"/datasets/isValid", bytes.NewBuffer(bmm))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusForbidden {
		return fmt.Errorf("metadata checking error - SciCat returned 403, user is likely not allowed to ingest datasets")
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("metadata checking error - unexpected status code: %d", resp.StatusCode)
	}

	// check response (if {"valid": true} then the metadata is correct)
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	var responseMap map[string]interface{}
	json.Unmarshal(body, &responseMap)
	isValid, ok := responseMap["valid"]
	if !ok {
		return fmt.Errorf("no 'valid' attribute was returned in JSON response")
	}

	switch v := isValid.(type) {
	case bool:
		if !v {
			return fmt.Errorf("metadata is not valid")
		}
	default:
		return fmt.Errorf("'valid' contains non-boolean value")
	}

	return nil
}

// GetSourceFolder gets the source folder from the metadata.
func GetSourceFolder(metaDataMap map[string]interface{}) (string, error) {
	sourceFolder := ""
	val, ok := metaDataMap["sourceFolder"]
	if !ok {
		return "", errors.New("undefined sourceFolder field")
	}

	sourceFolder, ok = val.(string)
	if !ok {
		return "", errors.New("sourceFolder is not a string")
	}

	// NOTE: this part seems very PSI specific
	// [if lvl.1 (or 2?) path == "sls" and lvl.3 (or 4?) path == "data"] => evaluate symlinks in path
	parts := strings.Split(sourceFolder, "/")
	if len(parts) > 3 && parts[3] == "data" && parts[1] == "sls" {
		var err error
		sourceFolder, err = filepath.EvalSymlinks(sourceFolder)
		if err != nil {
			return "", fmt.Errorf("failed to find canonical form of sourceFolder:%v %v", sourceFolder, err)
		}
		//log.Printf("Transform sourceFolder %v to canonical form: %v", val, sourceFolder)
		metaDataMap["sourceFolder"] = sourceFolder
	}

	return sourceFolder, nil
}
