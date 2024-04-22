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
	"regexp"
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
	rsyncFlags := []string{"-e", "ssh", "-avxz", "--progress", "--stderr=error"}
	if absFileListing != "" {
		rsyncFlags = append(rsyncFlags, "-r", "--files-from", absFileListing)
	}
	if version.Compare(versionNumber, "3.2.3", ">=") {
		rsyncCmd = exec.Command("/usr/bin/rsync", append(rsyncFlags, fullSourceFolderPath, serverConnectString)...)
	} else {
		rsyncCmd = exec.Command("/usr/bin/rsync", append(rsyncFlags, "-q", "--msgs2stderr", fullSourceFolderPath, serverConnectString)...)
	}
		
	// Show rsync's output	
	rsyncCmd.Stderr = os.Stderr
	log.Printf("Running: %v.\n", rsyncCmd.Args)
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
	version := string(output)
	
	// Use a regular expression to find the version number.
	// It will match the first occurrence of a string in the format "x.y.z" in the `version` string, where "x", "y", and "z" are one or more digits.
	re := regexp.MustCompile(`\d+\.\d+\.\d+`)
	versionNumber := re.FindString(version)
	if versionNumber == "" {
		return "", fmt.Errorf("could not find version number in rsync version string: %s", version)
	}
	
	return versionNumber, nil
}
