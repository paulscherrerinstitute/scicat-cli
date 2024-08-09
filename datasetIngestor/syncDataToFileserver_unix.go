//go:build aix || darwin || dragonfly || freebsd || (js && wasm) || linux || nacl || netbsd || openbsd || solaris
// +build aix darwin dragonfly freebsd js,wasm linux nacl netbsd openbsd solaris

// very important: there must be an empty line after the build flag line .
package datasetIngestor

import (
	"fmt"
	"io"
	"os/exec"
	"regexp"
	"strings"

	version "github.com/mcuadros/go-version"
)

// functionality needed for "de-central" data
// copies data from a local machine to a fileserver, uses RSync underneath
func SyncLocalDataToFileserver(datasetId string, user map[string]string, RSYNCServer string, sourceFolder string, absFileListing string, cmdOutput io.Writer) (err error) {
	username := user["username"]
	shortDatasetId := strings.Split(datasetId, "/")[1]
	//log.Println("short dataset id:", shortDatasetId)
	destFolder := "archive/" + shortDatasetId + sourceFolder
	serverConnectString := fmt.Sprintf("%s@%s:%s", username, RSYNCServer, destFolder)
	// append trailing slash to sourceFolder to indicate that the *contents* of the folder should be copied
	// no special handling for blanks in sourceFolder needed here
	fullSourceFolderPath := sourceFolder + "/"

	versionNumber, err := getRsyncVersion()
	if err != nil {
		return fmt.Errorf("error getting rsync version: %v", err)
	}

	rsyncCmd := buildRsyncCmd(versionNumber, absFileListing, fullSourceFolderPath, serverConnectString)

	// Show rsync's output
	rsyncCmd.Stdout = cmdOutput
	rsyncCmd.Stderr = cmdOutput
	fmt.Fprintf(cmdOutput, "Running: %v.\n", rsyncCmd.Args)
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

// Check rsync version and adjust command accordingly
func buildRsyncCmd(versionNumber, absFileListing, fullSourceFolderPath, serverConnectString string) *exec.Cmd {
	rsyncFlags := []string{"-e", "ssh", "-avx", "--progress"}
	if absFileListing != "" {
		rsyncFlags = append([]string{"-r", "--files-from", absFileListing}, rsyncFlags...)
	}
	if version.Compare(versionNumber, "3.2.3", ">=") {
		rsyncFlags = append(rsyncFlags, "--stderr=error")
		// Full command: /usr/bin/rsync -e ssh -avx --progress -r --files-from <absFileListing> --stderr=error <fullSourceFolderPath> <serverConnectString>
	} else {
		rsyncFlags = append(rsyncFlags, "-q", "--msgs2stderr")
		// Full command: /usr/bin/rsync -e ssh -avx --progress -r --files-from <absFileListing> -q --msgs2stderr <fullSourceFolderPath> <serverConnectString>
	}
	rsyncCmd := exec.Command("/usr/bin/rsync", append(rsyncFlags, fullSourceFolderPath, serverConnectString)...)
	return rsyncCmd
}
