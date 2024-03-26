package datasetUtils

import (
	"bufio"
	"log"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"
	"encoding/json"
	version "github.com/mcuadros/go-version"
	"fmt"
)

var scanner = bufio.NewScanner(os.Stdin)

var (
	GitHubAPI = "https://api.github.com/repos/paulscherrerinstitute/scicat-cli/releases/latest"
	DeployLocation = "https://github.com/paulscherrerinstitute/scicat-cli/releases/download"
)

type Release struct {
	TagName string `json:"tag_name"`
}

func fetchLatestVersion(client *http.Client) (string, error) {
	resp, err := client.Get(GitHubAPI)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	
	if resp.StatusCode != 200 {
		return "", fmt.Errorf("got %s fetching %s", resp.Status, GitHubAPI)
	}
	
	var release Release
	err = json.NewDecoder(resp.Body).Decode(&release)
	if err != nil {
		return "", err
	}
	
	return strings.TrimSpace(release.TagName), nil
}

// Make sure the version number is stripped of the 'v' prefix. That's required for `strconv.Atoi` to work.
func generateDownloadURL(deployLocation, latestVersion, osName string) string {
    if strings.ToLower(osName) == "windows" {
        return fmt.Sprintf("%s/v%s/scicat-cli_.%s_%s_x86_64.zip", deployLocation, latestVersion, latestVersion, strings.Title(osName))
    }
    return fmt.Sprintf("%s/v%s/scicat-cli_.%s_%s_x86_64.tar.gz", deployLocation, latestVersion, latestVersion, strings.Title(osName))
}

func CheckForNewVersion(client *http.Client, APP string, VERSION string, interactiveFlag bool) {
	latestVersion, err := fetchLatestVersion(client)
	if err != nil {
		log.Printf("Can not find info about latest version for this program: %s\n", err)
		return
	}
	
	latestVersion = strings.TrimPrefix(latestVersion, "v")
	majorLatest, err := strconv.Atoi(strings.Split(latestVersion, ".")[0])
	if err != nil {
		log.Fatalf("Illegal latest version number:%v", latestVersion)
	}
	majorCurrent, err := strconv.Atoi(strings.Split(VERSION, ".")[0])
	if err != nil {
		log.Fatalf("Illegal version number:%v", VERSION)
	}
	log.Printf("Latest version: %s", latestVersion)
	
	// Get the operating system name
	osName := runtime.GOOS
	
	// Generate the download URL
	downloadURL := generateDownloadURL(DeployLocation, latestVersion, osName)

	if majorLatest > majorCurrent || version.Compare(latestVersion, VERSION, ">") {
		log.Println("You should upgrade to a newer version")
		log.Println("Current Version: ", VERSION)
		log.Println("Latest  Version: ", latestVersion)
		log.Println("You can either download the file using the browser or use the following command:")

		if strings.ToLower(osName) == "windows" {
			log.Printf("Browser: %s\nCommand: curl -L -O %s; unzip scicat-cli_.%s_%s_x86_64.zip; cd scicat-cli\n", downloadURL, downloadURL, latestVersion, strings.Title(osName))
		} else {
			log.Printf("Browser: %s\nCommand: curl -L -O %s; tar xzf scicat-cli_.%s_%s_x86_64.tar.gz; cd scicat-cli; chmod +x %s\n", downloadURL, downloadURL, latestVersion, strings.Title(osName), APP)
		}

		if interactiveFlag {
			log.Print("Do you want to continue with current version (y/N) ? ")
			scanner.Scan()
			continueyn := scanner.Text()
			if continueyn != "y" {
				log.Fatalf("Execution stopped, please update program now.\n")
			}
		}
	} else {
		log.Println("Your version of this program is up-to-date")
	}
}
