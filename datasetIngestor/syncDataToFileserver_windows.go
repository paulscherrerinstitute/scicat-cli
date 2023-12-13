// functionality needed for "de-central" data
package datasetIngestor

import (
	"log"
	"path"
	"regexp"
	"strings"
)

func SyncDataToFileserver(datasetId string, user map[string]string, RSYNCServer string, sourceFolder string, absFileListing string) (err error) {

	username := user["username"]
	password := user["password"]
	shortDatasetId := strings.Split(datasetId, "/")[1]
	// remove leading "C:"" if existing etc
	ss := strings.Split(sourceFolder, ":")
	// construct destination folder from sourceFolder, sourceFolder allowed to have Windows backslash in folder name
	destFull := ss[len(ss)-1]
	separator := "/"
	if strings.Index(destFull, "/") < 0 {
		separator = "\\"
	}
	destparts := strings.Split(destFull, separator)

	destFolder := "archive/" + shortDatasetId + strings.Join(destparts[0:len(destparts)-1], "/")
	destFolder2 := "archive/" + shortDatasetId + strings.Join(destparts[0:len(destparts)], "/")

	// fmt.Println("Destination folder:", destFolder)
	// fmt.Println("Sourcefolder:", sourceFolder)

	// add port number if missing
	FullRSYNCServer := RSYNCServer
	if !strings.Contains(RSYNCServer, ":") {
		FullRSYNCServer = RSYNCServer + ":22"
	}

	c, err := NewDumbClient(username, password, FullRSYNCServer)

	if err != nil {
		return err
	}

	c.Quiet = false
	c.PreseveTimes = true
	re := regexp.MustCompile(`^\/([A-Z])\/`)

	// now copy recursively: either just one sourceFolder or all entries inside absFileListing
	// Note: destfolder must exist before, needs dedicated scp server support

	if absFileListing != "" {
		lines, err := readLines(absFileListing)
		if err != nil {
			log.Fatalf("Could not read filellist, readLines: %s", err)
		}
		for _, line := range lines {
			windowsSource := re.ReplaceAllString(path.Join(sourceFolder, line), "$1:/")
			// log.Printf("Copying data via scp from %s to %s\n", windowsSource, destFolder2)
			err = c.Send(destFolder2, windowsSource)
		}
	} else {
		windowsSource := re.ReplaceAllString(sourceFolder, "$1:/")
		// log.Printf("Copying data via scp from %s to %s\n", windowsSource, destFolder)
		err = c.Send(destFolder, windowsSource)
	}
	return err
}
