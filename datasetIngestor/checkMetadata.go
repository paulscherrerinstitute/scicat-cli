package datasetIngestor

import (
	"bytes"
	"scicat/datasetUtils"
	"encoding/json"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/fatih/color"
)

const DUMMY_TIME = "2300-01-01T11:11:11.000Z"
const DUMMY_OWNER = "x12345"

func getHost() string {
	hostname, err := os.Hostname()
	if err != nil {
		return "unknown"
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

func CheckMetadata(client *http.Client, APIServer string, metadatafile string, user map[string]string,
	accessGroups []string) (metaDataMap map[string]interface{}, sourceFolder string, beamlineAccount bool) {

	// read full meta data
	b, err := ioutil.ReadFile(metadatafile) // just pass the file name
	if err != nil {
		log.Fatal(err)
	}

	var metadataObj interface{}
	err = json.Unmarshal(b, &metadataObj)
	if err != nil {
		log.Fatal(err)
	}
	// use type assertion to access f's underlying map
	metaDataMap = metadataObj.(map[string]interface{})
	beamlineAccount = false

	if user["displayName"] != "ingestor" {
		if ownerGroup, ok := metaDataMap["ownerGroup"]; ok {
			validOwner := false
			for _, b := range accessGroups {
				if b == ownerGroup {
					validOwner = true
					break
				}
			}
			if validOwner {
				log.Printf("OwnerGroup information %s verified successfully.\n", ownerGroup)
			} else {
				// check for beamline specific account if raw data
				if creationLocation, ok := metaDataMap["creationLocation"]; ok {
					parts := strings.Split(creationLocation.(string), "/")
					expectedAccount := ""
					if len(parts) == 4 {
						expectedAccount = strings.ToLower(parts[2]) + strings.ToLower(parts[3])
					}
					if user["displayName"] == expectedAccount {
						log.Printf("Beamline specific dataset %s - ingest granted.\n", expectedAccount)
						beamlineAccount = true
					} else {
						log.Fatalf("You are neither member of the ownerGroup %s nor the needed beamline account %s", ownerGroup, expectedAccount)
					}
				} else {
					// for other data just check user name
					// this is a quick and dirty test. Should be replaced by test for "globalaccess" role
					// facilities: ["SLS", "SINQ", "SWISSFEL", "SmuS"],
					u := user["displayName"]
					if strings.HasPrefix(u, "sls") ||
						strings.HasPrefix(u, "swissfel") ||
						strings.HasPrefix(u, "sinq") ||
						strings.HasPrefix(u, "smus") {
						beamlineAccount = true
					}
				}
			}
		}
	}

	// Check if ownerGroup is in accessGroups list

	color.Set(color.FgGreen)
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
		if hostname == "unknown" {
			log.Printf("sourceFolderHost is unknown")
		} else {
			metaDataMap["sourceFolderHost"] = hostname
			log.Printf("sourceFolderHost field added: %s", metaDataMap["sourceFolderHost"])
		}
	}
	// far raw data add PI if missing
	if val, ok := metaDataMap["type"]; ok {
		dstype := val.(string)
		if dstype == "raw" {
			if _, ok := metaDataMap["principalInvestigator"]; !ok {
				val, ok := metaDataMap["ownerGroup"]
				if ok {
					ownerGroup := val.(string)
					proposal := datasetUtils.GetProposal(client, APIServer, ownerGroup, user, accessGroups)
					if val, ok := proposal["pi_email"]; ok {
						metaDataMap["principalInvestigator"] = val.(string)
						log.Printf("principalInvestigator field added: %s", metaDataMap["principalInvestigator"])
					} else {
						color.Set(color.FgRed)
						log.Printf("principalInvestigator field missing for raw data and could not be added from proposal data.")
						log.Printf("Please add the field explicitly to metadata file")
						color.Unset()
					}
				}
			}
		}
	}

	color.Unset()
	var bmm []byte
	if val, ok := metaDataMap["type"]; ok {
		dstype := val.(string)
		// fmt.Println(errm,sourceFolder)

		// verify data structure of meta data by calling isValid API for Dataset

		myurl := ""
		if dstype == "raw" {
			myurl = APIServer + "/RawDatasets/isValid"
		} else if dstype == "derived" {
			myurl = APIServer + "/DerivedDatasets/isValid"
		} else if dstype == "base" {
			myurl = APIServer + "/Datasets/isValid"
		} else {
			log.Fatal("Unknown dataset type encountered:", dstype)
		}

		// add dummy data for fields which can only be filled after file scan to pass the validity test

		if _, exists := metaDataMap["ownerGroup"]; !exists {
			metaDataMap["ownerGroup"] = DUMMY_OWNER
		}
		if _, exists := metaDataMap["creationTime"]; !exists {
			metaDataMap["creationTime"] = DUMMY_TIME
		}
		if metaDataMap["type"] == "raw" {
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

		bmm, _ = json.Marshal(metaDataMap)
		//fmt.Printf("Marshalled meta data : %s\n", string(bmm))
		// now check validity
		req, err := http.NewRequest("POST", myurl, bytes.NewBuffer(bmm))
		req.Header.Set("Content-Type", "application/json")

		resp, err := client.Do(req)
		if err != nil {
			log.Fatal(err)
		}
		defer resp.Body.Close()

		body, _ := ioutil.ReadAll(resp.Body)

		// check validity
		var respObj interface{}
		err = json.Unmarshal(body, &respObj)
		if err != nil {
			log.Fatal(err)
		}
		respMap := respObj.(map[string]interface{})
		if respMap["valid"] != true {
			log.Fatal("response Body:", string(body))
		}
	} else {
		log.Fatal("Undefined type field")
	}

	sourceFolder = ""
	if val, ok := metaDataMap["sourceFolder"]; ok {
		// turn sourceFolder into canonical form but only for online data /sls/BL/data form
		sourceFolder = val.(string)
		var parts = strings.Split(val.(string), "/")
		if len(parts) > 3 && parts[3] == "data" && parts[1] == "sls" {
			sourceFolder, err = filepath.EvalSymlinks(val.(string))
			if err != nil {
				log.Fatalf("Failed to find canonical form of sourceFolder:%v %v", val, err)
			}
			color.Set(color.FgYellow)
			log.Printf("Transform sourceFolder %v to canonical form: %v", val, sourceFolder)
			color.Unset()
			metaDataMap["sourceFolder"] = sourceFolder
		}
	} else {
		log.Fatal("Undefined sourceFolder field")
	}
	return metaDataMap, sourceFolder, beamlineAccount

}
