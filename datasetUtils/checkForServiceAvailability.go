package datasetUtils

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"gopkg.in/yaml.v2"
	"github.com/fatih/color"
	"time"
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

var GitHubMainLocation = "https://raw.githubusercontent.com/paulscherrerinstitute/scicat-cli/service-availability"

// CheckForServiceAvailability checks the availability of the dataset ingestor service.
// It fetches a YAML file from GitHubMainLocation, parses it, and logs the service availability status.
func CheckForServiceAvailability(client *http.Client, testenvFlag bool, autoarchiveFlag bool) {
	s, err := getServiceAvailability(client)
	if err != nil {
		log.Printf("Failed to get service availability: %v", err)
		return
	}
	
	status, env := determineStatusAndEnv(s, testenvFlag)
	
	logPlannedDowntime(status, env)
	
	err = handleServiceUnavailability(status, env, autoarchiveFlag)
	if err != nil {
			log.Printf("Error: %v", err)
			os.Exit(1)
	}
}

func getServiceAvailability(client *http.Client) (ServiceAvailability, error) {
	yamlFile, err := readYAMLFile(client)
	if err != nil {
		return ServiceAvailability{}, err
	}
	
	s := ServiceAvailability{}
	err = yaml.Unmarshal(yamlFile, &s)
	if err != nil {
		return ServiceAvailability{}, fmt.Errorf("Unmarshal of availability file failed: %v\n%s", err, yamlFile)
	}
	
	return s, nil
}

func determineStatusAndEnv(s ServiceAvailability, testenvFlag bool) (OverallAvailability, string) {
	status := OverallAvailability{Availability{"on", "", "", ""}, Availability{"on", "", "", ""}}
	env := "production"
	
	if testenvFlag {
		if (OverallAvailability{}) != s.Qa {
			status = s.Qa
		}
		env = "test"
		} else {
		if (OverallAvailability{}) != s.Production {
				status = s.Production
		}	
	}
		
	return status, env
}

func logPlannedDowntime(status OverallAvailability, env string) {
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
}

func handleServiceUnavailability(status OverallAvailability, env string, autoarchiveFlag bool) error {
	// If the ingest service is not available, log a message and return an error
	if status.Ingest.Status != "on" {
		logServiceUnavailability("ingest", env, status.Ingest)
		return fmt.Errorf("ingest service is unavailable")
	}
	
	// If the archive service is not available and autoarchiveFlag is set, log a message and return an error
	if autoarchiveFlag && status.Archive.Status != "on" {
		logServiceUnavailability("archive", env, status.Archive)
		return fmt.Errorf("archive service is unavailable")
	}

	return nil
}

func logServiceUnavailability(serviceName string, env string, availability Availability) {
	color.Set(color.FgRed)
	log.Printf("The %s data catalog is currently not available for %sing new datasets\n", env, serviceName)
	log.Printf("Planned downtime until %v. Reason: %s\n", availability.Downto, availability.Comment)
	color.Unset()
}

func readYAMLFile(client *http.Client) ([]byte, error) {
	// Construct the URL of the service availability YAML file
	yamlURL := fmt.Sprintf("%s/cmd/datasetIngestor/datasetIngestorServiceAvailability.yml", GitHubMainLocation)
	
	ctx, cancel := context.WithTimeout(context.Background(), 5 * time.Second)
    defer cancel()

    req, err := http.NewRequestWithContext(ctx, "GET", yamlURL, nil)
    if err != nil {
        return "", err
    }

    resp, err := client.Do(req)	if err != nil {
		fmt.Println("No Information about Service Availability")
		return nil, fmt.Errorf("failed to fetch the service availability YAML file: %w", err)
	}
	defer resp.Body.Close()
	
	// If the HTTP status code is not 200 (OK), log a message and return
	if resp.StatusCode != 200 {
		log.Println("No Information about Service Availability")
		log.Printf("Error: Got %s fetching %s\n", resp.Status, yamlURL)
		return nil, fmt.Errorf("got %s fetching %s", resp.Status, yamlURL)
	}
	
	// Read the entire body of the response (the YAML file)
	yamlFile, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("Can not read service availability file for this application")
	}
	
	return yamlFile, nil
}
