// +build aix darwin dragonfly freebsd js,wasm linux nacl netbsd openbsd solaris

// very important: there must be an empty line after the build flag line .
package datasetIngestor

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
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
	// check if filelisting given
	// rsync can create only one level deep directory structure, here we need more, therefore mkdir -p
	// This code is no longer needed, sine Edgar has a new rrsync wrapper which craetes the needed directory
	// cmd := exec.Command("/usr/bin/ssh",RSYNCServer,"mkdir","-p",destFolder)
	// // show rsync's output
	// cmd.Stdout = os.Stdout
	// cmd.Stderr = os.Stderr
	//
	// fmt.Printf("Running %v.\n", cmd.Args)
	// cmd.Run()

	cmd := exec.Command("/usr/bin/rsync", "-e", "ssh -q", "-avxz", "--progress", "--msgs2stderr", fullSourceFolderPath, serverConnectString)
	// // TODO: create folderstructure mkdir -p also for this case:
	if absFileListing != "" {
		cmd = exec.Command("/usr/bin/rsync", "-e", "ssh -q", "-avxzr", "--progress", "--msgs2stderr", "--files-from", absFileListing, fullSourceFolderPath, serverConnectString)
	}
	// show rsync's output
	// cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	log.Printf("Running %v.\n", cmd.Args)
	err = cmd.Run()
	return err
}
