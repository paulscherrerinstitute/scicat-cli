package datasetUtils

import (
	"bufio"
	"io"
	"log"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"

	version "github.com/mcuadros/go-version"
)

var scanner = bufio.NewScanner(os.Stdin)

// check version of program, give hint to update
const DeployLocation = "https://gitlab.psi.ch/scicat/tools/raw/master/" + runtime.GOOS + "/"

func CheckForNewVersion(client *http.Client, APP string, VERSION string, interactiveFlag bool) {
	resp, err := client.Get(DeployLocation + APP + "Version.txt")
	if err != nil {
		log.Println("Can not find info about latest version for this program")
		log.Printf("Error: %s\n", err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		log.Println("Can not find info about latest version for this program")
		log.Printf("Error: Got %s fetching %s\n", resp.Status, DeployLocation + APP + "Version.txt")
		return
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Println("Can not read version file for this application")
		return
	}

	latestVersion := strings.TrimSpace(string(body))

	majorLatest, err := strconv.Atoi(strings.Split(latestVersion, ".")[0])
	if err != nil {
		log.Fatalf("Illegal latest version number:%v", latestVersion)
	}
	majorCurrent, err := strconv.Atoi(strings.Split(VERSION, ".")[0])
	if err != nil {
		log.Fatalf("Illegal version number:%v", VERSION)
	}
	log.Printf("Latest version: %s", latestVersion)
	if majorLatest > majorCurrent {
		log.Println("You must upgrade to a newer version")
		log.Println("Current Version: ", VERSION)
		log.Println("Latest  Version: ", latestVersion)
		log.Println("Use the following command to update:")
		log.Println("curl -O " + DeployLocation + APP + ";chmod +x " + APP)
		log.Fatalf("Execution stopped, please update program now.\n")
	} else if version.Compare(latestVersion, VERSION, ">") {
		log.Println("You should upgrade to a newer version")
		log.Println("Current Version: ", VERSION)
		log.Println("Latest  Version: ", latestVersion)
		log.Println("Use the following command to update:")
		log.Println("curl -O " + DeployLocation + APP + ";chmod +x " + APP)
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
