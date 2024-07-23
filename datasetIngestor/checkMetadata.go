package datasetIngestor

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/fatih/color"
	"github.com/paulscherrerinstitute/scicat/datasetUtils"
)

const (
	ErrIllegalKeys = "metadata contains keys with illegal characters (., [], $, or <>)"
	DUMMY_TIME     = "2300-01-01T11:11:11.000Z"
	DUMMY_OWNER    = "x12345"
)

const unknown = "unknown"
const raw = "raw"

func CheckMetadata(client *http.Client, APIServer string, metadatafile string, user map[string]string, accessGroups []string) (metaDataMap map[string]interface{}, sourceFolder string, beamlineAccount bool, err error) {
	metaDataMap, err = readMetadataFromFile(metadatafile)
	if err != nil {
		return nil, "", false, err
	}

	if keys := collectIllegalKeys(metaDataMap); len(keys) > 0 {
		return nil, "", false, errors.New(ErrIllegalKeys + ": \"" + strings.Join(keys, "\", \"") + "\"")
	}

	beamlineAccount, err = checkUserAndOwnerGroup(user, accessGroups, metaDataMap)
	if err != nil {
		return nil, "", false, err
	}

	err = augmentMissingMetadata(user, metaDataMap, client, APIServer, accessGroups)
	if err != nil {
		return nil, "", false, err
	}

	err = checkMetadataValidity(client, APIServer, metaDataMap)
	if err != nil {
		return nil, "", false, err
	}

	sourceFolder, err = getSourceFolder(metaDataMap)
	if err != nil {
		return nil, "", false, err
	}

	return metaDataMap, sourceFolder, beamlineAccount, nil
}

// readMetadataFromFile reads the metadata from the file and unmarshals it into a map.
func readMetadataFromFile(metadatafile string) (map[string]interface{}, error) {
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
func collectIllegalKeys(metadata map[string]interface{}) []string {
	stack := []map[string]interface{}{metadata}
	keys := []string{}

	for len(stack) > 0 {
		item := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		for key, value := range item {
			if containsIllegalCharacters(key) {
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

func containsIllegalCharacters(s string) bool {
	// Check if the string contains periods, brackets, or other illegal characters
	// You can adjust this condition based on your specific requirements
	for _, char := range s {
		if char == '.' || char == '[' || char == ']' || char == '<' || char == '>' || char == '$' {
			return true
		}
	}
	return false
}

// checkUserAndOwnerGroup checks the user and owner group and returns whether the user is a beamline account.
func checkUserAndOwnerGroup(user map[string]string, accessGroups []string, metaDataMap map[string]interface{}) (bool, error) {
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
			log.Printf("OwnerGroup information %s verified successfully.\n", ownerGroup)
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
			log.Printf("Beamline specific dataset %s - ingest granted.\n", expectedAccount)
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

// getHost is a function that attempts to retrieve and return the fully qualified domain name (FQDN) of the current host.
// If it encounters any error during the process, it gracefully falls back to returning the simple hostname or "unknown".
func getHost() string {
	// Try to get the hostname of the current machine.
	hostname, err := os.Hostname()
	if err != nil {
		return unknown
	}

	addrs, err := net.LookupIP(hostname)
	if err != nil {
		return hostname
	}

	for _, addr := range addrs {
		if ipv4 := addr.To4(); ipv4 != nil {
			ip, err := ipv4.MarshalText()
			if err != nil {
				return hostname
			}
			hosts, err := net.LookupAddr(string(ip))
			if err != nil || len(hosts) == 0 {
				return hostname
			}
			fqdn := hosts[0]
			return strings.TrimSuffix(fqdn, ".") // return fqdn without trailing dot
		}
	}
	return hostname
}

// augmentMissingMetadata augments missing metadata fields.
func augmentMissingMetadata(user map[string]string, metaDataMap map[string]interface{}, client *http.Client, APIServer string, accessGroups []string) error {
	color.Set(color.FgGreen)
	defer color.Unset()

	// optionally augment missing owner metadata
	if _, ok := metaDataMap["owner"]; !ok {
		metaDataMap["owner"] = user["displayName"]
		log.Printf("owner field added: %s", metaDataMap["owner"])
	}
	if _, ok := metaDataMap["ownerEmail"]; !ok {
		metaDataMap["ownerEmail"] = user["mail"]
		log.Printf("ownerEmail field added: %s", metaDataMap["ownerEmail"])
	}
	if _, ok := metaDataMap["contactEmail"]; !ok {
		metaDataMap["contactEmail"] = user["mail"]
		log.Printf("contactEmail field added: %s", metaDataMap["contactEmail"])
	}

	// and sourceFolderHost
	if _, ok := metaDataMap["sourceFolderHost"]; !ok {
		hostname := getHost()
		if hostname == unknown {
			log.Printf("sourceFolderHost is unknown")
		} else {
			metaDataMap["sourceFolderHost"] = hostname
			log.Printf("sourceFolderHost field added: %s", metaDataMap["sourceFolderHost"])
		}
	}

	// for raw data add PI if missing
	if err := addPrincipalInvestigatorFromProposal(user, metaDataMap, client, APIServer, accessGroups); err != nil {
		return err
	}

	return nil
}

func addPrincipalInvestigatorFromProposal(user map[string]string, metaDataMap map[string]interface{}, client *http.Client, APIServer string, accessGroups []string) error {
	typeVal, ok := metaDataMap["type"]
	if !ok {
		return fmt.Errorf("type doesn't exist as an attribute")
	}
	dstype, ok := typeVal.(string)
	if !ok {
		return fmt.Errorf("type is not a string")
	}
	if dstype != raw {
		// exit if not raw
		return nil
	}

	if _, ok := metaDataMap["principalInvestigator"]; ok {
		// exit if present
		return nil
	}

	val, ok := metaDataMap["ownerGroup"]
	if !ok {
		return fmt.Errorf("ownerGroup is not present in metadata attributes")
	}

	ownerGroup, ok := val.(string)
	if !ok {
		return fmt.Errorf("ownerGroup is not a string")
	}

	proposal, err := datasetUtils.GetProposal(client, APIServer, ownerGroup, user, accessGroups)
	if err != nil {
		return fmt.Errorf("error: %v", err)
	}

	if val, ok := proposal["pi_email"]; ok {
		piEmail, ok := val.(string)
		if !ok {
			return fmt.Errorf("pi_email is not a string")
		}
		metaDataMap["principalInvestigator"] = piEmail
		log.Printf("principalInvestigator field added: %s", metaDataMap["principalInvestigator"])
	} else {
		log.Printf("principalInvestigator field missing for raw data and could not be added from proposal data.")
		log.Printf("Please add the field explicitly to metadata file")
	}
	return nil
}

// checkMetadataValidity checks the validity of the metadata by calling the appropriate API.
func checkMetadataValidity(client *http.Client, APIServer string, metaDataMap map[string]interface{}) error {
	dstype, ok := metaDataMap["type"].(string)
	if !ok {
		return fmt.Errorf("metadata type isn't a string")
	}

	myurl := ""
	switch dstype {
	case raw:
		myurl = APIServer + "/RawDatasets/isValid"
	case "derived":
		myurl = APIServer + "/DerivedDatasets/isValid"
	case "base":
		myurl = APIServer + "/Datasets/isValid"
	default:
		return fmt.Errorf("unknown dataset type encountered: %s", dstype)
	}

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

	// add accessGroups entry for beamline if creationLocation is defined

	if value, exists := metaDataMap["creationLocation"]; exists {
		var parts = strings.Split(value.(string), "/")
		var groups []string
		if len(parts) == 4 {
			newGroup := strings.ToLower(parts[2]) + strings.ToLower(parts[3])

			if ag, exists := metaDataMap["accessGroups"]; exists {
				// a direct typecast does not work, this loop is needed
				aInterface := ag.([]interface{})
				aString := make([]string, len(aInterface))
				for i, v := range aInterface {
					aString[i] = v.(string)
				}
				groups = append(aString, newGroup)
			} else {
				groups = append(groups, newGroup)
			}
		}
		metaDataMap["accessGroups"] = groups
	}

	bmm, err := json.Marshal(metaDataMap)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", myurl, bytes.NewBuffer(bmm))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	_, err = io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	return nil
}

// getSourceFolder gets the source folder from the metadata.
func getSourceFolder(metaDataMap map[string]interface{}) (string, error) {
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
	parts := strings.Split(sourceFolder, "/")
	if len(parts) > 3 && parts[3] == "data" && parts[1] == "sls" {
		var err error
		sourceFolder, err = filepath.EvalSymlinks(sourceFolder)
		if err != nil {
			return "", fmt.Errorf("failed to find canonical form of sourceFolder:%v %v", sourceFolder, err)
		}
		log.Printf("Transform sourceFolder %v to canonical form: %v", val, sourceFolder)
		metaDataMap["sourceFolder"] = sourceFolder
	}

	return sourceFolder, nil
}
