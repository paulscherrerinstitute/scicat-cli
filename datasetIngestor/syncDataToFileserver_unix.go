// +build aix darwin dragonfly freebsd js,wasm linux nacl netbsd openbsd solaris

// very important: there must be an empty line after the build flag line .
package datasetIngestor

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
	version "github.com/mcuadros/go-version"
)

// functionality needed for "de-central" data
func SyncDataToFileserver(datasetId string, user map[string]string, RSYNCServer string, sourceFolder string, absFileListing string) (err error) {
	username := user["username"]
	shortDatasetId := strings.Split(datasetId, "/")[1]
	log.Println("short dataset id:", shortDatasetId)
	destFolder := "archive/" + shortDatasetId + sourceFolder
	serverConnectString := fmt.Sprintf("%s@%s:%s", username, RSYNCServer, destFolder)
	// append trailing slash to sourceFolder to indicate that the *contents* of the folder should be copied
	// no special handling for blanks in sourceFolder needed here
	fullSourceFolderPath := sourceFolder + "/"
	
	versionNumber, err := getRsyncVersion()
	if err != nil {
		log.Fatal("Error getting rsync version: ", err)
	}
	
	// Check rsync version and adjust command accordingly
	var rsyncCmd *exec.Cmd
	if version.Compare(versionNumber, "3.2.3", ">=") {
		rsyncCmd = exec.Command("/usr/bin/rsync", "-e", "ssh", "-avxz", "--progress", "--stderr=error", fullSourceFolderPath, serverConnectString)
		if absFileListing != "" {
			rsyncCmd = exec.Command("/usr/bin/rsync", "-e", "ssh", "-avxzr", "--progress", "--stderr=error", "--files-from", absFileListing, fullSourceFolderPath, serverConnectString)
		}
	} else {
		rsyncCmd = exec.Command("/usr/bin/rsync", "-e", "ssh -q", "-avxz", "--progress", "--msgs2stderr", fullSourceFolderPath, serverConnectString)
		if absFileListing != "" {
			rsyncCmd = exec.Command("/usr/bin/rsync", "-e", "ssh -q", "-avxzr", "--progress", "--msgs2stderr", "--files-from", absFileListing, fullSourceFolderPath, serverConnectString)
		}
	}
		
	// Show rsync's output	
	rsyncCmd.Stderr = os.Stderr
	log.Printf("Running %v.\n", rsyncCmd.Args)
	err = rsyncCmd.Run()
	return err
}

// Get rsync version
func getRsyncVersion() (string, error) {
	cmd := exec.Command("/usr/bin/rsync", "--version")
	output, err := cmd.Output()
	if err != nil {
		return "", err
	}
	version := strings.Split(string(output), "\n")[0]
	versionNumber := strings.Split(version, " ")[2]
	return versionNumber, nil
}
