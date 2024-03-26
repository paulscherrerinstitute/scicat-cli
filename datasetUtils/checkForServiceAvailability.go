package datasetUtils

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"os"

	"gopkg.in/yaml.v2"

	"github.com/fatih/color"
)

type Availability struct {
	Status   string `yaml:"status"`
	Downfrom string `yaml:"downfrom"`
	Downto   string `yaml:"downto"`
	Comment  string `yaml:"comment"`
}

type OverallAvailability struct {
	Ingest  Availability
	Archive Availability
}
type ServiceAvailability struct {
	Production OverallAvailability
	Qa         OverallAvailability
}

// CheckForServiceAvailability checks the availability of the dataset ingestor service.
// It fetches a YAML file from a specified location, parses it, and logs the service availability status.
func CheckForServiceAvailability(client *http.Client, testenvFlag bool, autoarchiveFlag bool) {
	// Send a GET request to fetch the service availability YAML file
	resp, err := client.Get(DeployLocation + "datasetIngestorServiceAvailability.yml")
	if err != nil {
		fmt.Println("No Information about Service Availability")
		return
	}
	defer resp.Body.Close()

	// If the HTTP status code is not 200 (OK), log a message and return
	if resp.StatusCode != 200 {
		log.Println("No Information about Service Availability")
		log.Printf("Error: Got %s fetching %s\n", resp.Status, DeployLocation + "datasetIngestorServiceAvailability.yml")
		return
	}

	// Read the entire body of the response (the YAML file)
	yamlFile, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("Can not read service availability file for this application")
		return
	}

	// Unmarshal the YAML file into a ServiceAvailability struct
	s := ServiceAvailability{}
	err = yaml.Unmarshal(yamlFile, &s)
	if err != nil {
		log.Fatalf("Unmarshal of availabilty file failed: %v\n%s", err, yamlFile)
	}

	// Determine the service status and environment based on the testenvFlag
	var status OverallAvailability
	var env string
	// define default value
	status = OverallAvailability{Availability{"on", "", "", ""}, Availability{"on", "", "", ""}}
	if testenvFlag {
		if (OverallAvailability{}) != s.Qa {
			status = s.Qa
		}
		env = "test"
	} else {
		if (OverallAvailability{}) != s.Production {
			status = s.Production
		}
		env = "production"
	}

	// Reset the terminal color after the function returns
	defer color.Unset()

	// Log the planned downtime for the ingest and archive services, if any
	if status.Ingest.Downfrom != "" {
		color.Set(color.FgYellow)
		fmt.Printf("Next planned downtime for %s data catalog ingest service is scheduled at %v\n", env, status.Ingest.Downfrom)
	}
	if status.Ingest.Downto != "" {
		color.Set(color.FgYellow)
		fmt.Printf("It is scheduled to last until %v\n", status.Ingest.Downto)
	}
	if status.Archive.Downfrom != "" {
		color.Set(color.FgYellow)
		fmt.Printf("Next planned downtime for %s data catalog archive service is scheduled at %v\n", env, status.Archive.Downfrom)
	}
	if status.Archive.Downto != "" {
		color.Set(color.FgYellow)
		fmt.Printf("It is scheduled to last until %v\n", status.Archive.Downto)
	}

	// If the ingest service is not available, log a message and terminate the program
	if status.Ingest.Status != "on" {
		color.Set(color.FgRed)
		log.Printf("The %s data catalog is currently not available for ingesting new datasets\n", env)
		log.Printf("Planned downtime until %v. Reason: %s\n", status.Ingest.Downto, status.Ingest.Comment)
		color.Unset()
		os.Exit(1)
	}

	// If the archive service is not available and autoarchiveFlag is set, log a message and terminate the program
	if autoarchiveFlag && status.Archive.Status != "on" {
		color.Set(color.FgRed)
		log.Printf("The %s data catalog is currently not available for archiving new datasets\n", env)
		log.Printf("Planned downtime until %v. Reason: %s\n", status.Archive.Downto, status.Archive.Comment)
		color.Unset()
		os.Exit(1)
	}
}
