package datasetIngestor

import (
	"log"
	"os"
	"os/exec"
)

func TestDataCentrallyAvailable(username string, ARCHIVEServer string, sourceFolder string) (err error) {

	cmd := exec.Command("/usr/bin/ssh", "-q", "-l", username, ARCHIVEServer, "test", "-d", sourceFolder)
	// show rsync's output
	//cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	log.Printf("Running %v.\n", cmd.Args)
	err = cmd.Run()
	return err
}
