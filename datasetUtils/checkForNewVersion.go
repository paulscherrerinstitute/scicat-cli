package datasetUtils

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	version "github.com/mcuadros/go-version"
)

var scanner = bufio.NewScanner(os.Stdin)

var (
	GitHubAPI      = "https://api.github.com/repos/paulscherrerinstitute/scicat-cli/releases/latest"
	DonwloadInstructions = "https://github.com/paulscherrerinstitute/scicat-cli?tab=readme-ov-file#manual-deployment-and-upgrade"
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

func CheckForNewVersion(client *http.Client, APP string, VERSION string)  {
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

	if !version.Compare(latestVersion, VERSION, ">") {
		log.Println("Your version of this program is up-to-date")
		return
	}

	// Notify an update if the version has changed
	log.Println("You should upgrade to a newer version")
	log.Println("Current Version:", VERSION)
	log.Println("Latest  Version:", latestVersion)
	log.Println("You can find the download instructions here:", DonwloadInstructions)
}
