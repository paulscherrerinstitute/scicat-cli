package datasetUtils

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"
	version "github.com/mcuadros/go-version"
)

var scanner = bufio.NewScanner(os.Stdin)

var (
	GitHubAPI      = "https://api.github.com/repos/paulscherrerinstitute/scicat-cli/releases/latest"
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

func CheckForNewVersion(client *http.Client, APP string, VERSION string, interactiveFlag bool, userInput UserInput)  {
	// avoid checking for new version in test mode
	if os.Getenv("TEST_MODE") == "true" {
		return
	}
	latestVersion, err := fetchLatestVersion(client)
	if err != nil {
			log.Printf("Warning: Can not find info about latest version for this program: %s\n", err)
			return
	}

	latestVersion = strings.TrimPrefix(latestVersion, "v")
	_, err = strconv.Atoi(strings.Split(latestVersion, ".")[0])
	if err != nil {
		log.Printf("Warning: Illegal latest version number:%v\n", latestVersion)
	}
	_, err = strconv.Atoi(strings.Split(VERSION, ".")[0])
	if err != nil {
		log.Printf("Warning: Illegal version number:%v\n", VERSION)
	}
	log.Printf("Latest version: %s", latestVersion)

	// Get the operating system name
	osName := runtime.GOOS

	// Generate the download URL
	downloadURL := generateDownloadURL(DeployLocation, latestVersion, osName)

	// Split the versions into parts
	currentParts := strings.Split(VERSION, ".")
	latestParts := strings.Split(latestVersion, ".")

	// Convert the major and minor parts to integers
	currentMajor, _ := strconv.Atoi(currentParts[0])
	currentMinor, _ := strconv.Atoi(currentParts[1])
	latestMajor, _ := strconv.Atoi(latestParts[0])
	latestMinor, _ := strconv.Atoi(latestParts[1])

	if version.Compare(latestVersion, VERSION, ">") {
		// Notify an update if the version has changed
		log.Println("You should upgrade to a newer version")
		log.Println("Current Version: ", VERSION)
		log.Println("Latest  Version: ", latestVersion)
		log.Println("You can either download the file using the browser or use the following command:")

		if strings.ToLower(osName) == "windows" {
			log.Printf("Browser: %s\nCommand: curl -L -O %s; unzip scicat-cli_.%s_%s_x86_64.zip; cd scicat-cli\n", downloadURL, downloadURL, latestVersion, strings.Title(osName))
		} else {
			log.Printf("Browser: %s\nCommand: curl -L -O %s; tar xzf scicat-cli_.%s_%s_x86_64.tar.gz; cd scicat-cli; chmod +x %s\n", downloadURL, downloadURL, latestVersion, strings.Title(osName), APP)
		}
	} else {
		log.Println("Your version of this program is up-to-date")
	}
	if interactiveFlag && (latestMajor > currentMajor || latestMinor > currentMinor) {
		log.Print("Do you want to continue with current version (y/N) ? ")
		continueyn, err := userInput.ReadLine()
		if err != nil {
				log.Printf("Warning: Failed to read user input: %v\n", err)
		} else if strings.TrimSpace(continueyn) != "y" {
				log.Println("Warning: Execution stopped, please update the program now.")
		}
	}
	return
}

// UserInput is an interface that defines a method to read a line of input. We use this so that we can test interactive mode.
type UserInput interface {
	ReadLine() (string, error)
}

type StdinUserInput struct{}

func (StdinUserInput) ReadLine() (string, error) {
	reader := bufio.NewReader(os.Stdin)
	return reader.ReadString('\n')
}
