package datasetIngestor

import (
	"log"
	"os"
	"os/exec"
)

// execCommand is a variable that points to exec.Command, allowing it to be replaced in tests.
var execCommand = exec.Command

// TestDataCentrallyAvailable checks if a specific directory (sourceFolder) is available on a remote server (ARCHIVEServer) 
// using the provided username for SSH connection. It returns an error if the directory is not available or if there's an issue with the SSH connection.
func TestDataCentrallyAvailable(username string, ARCHIVEServer string, sourceFolder string) (err error) {
	
	// Create a new exec.Command to run the SSH command. The command checks if the directory exists on the remote server.
	// The "-q" option suppresses all warnings, "-l" specifies the login name on the remote server.
	cmd := execCommand("/usr/bin/ssh", "-q", "-l", username, ARCHIVEServer, "test", "-d", sourceFolder)
	
	// Redirect the command's standard error to the process's standard error.
	// This means that any error messages from the command will be displayed in the terminal.
	cmd.Stderr = os.Stderr
	
	// Log the command that is being run for debugging purposes.
	log.Printf("Running %v.\n", cmd.Args)
	
	// Run the command and return any error that occurs.
	err = cmd.Run()
	return err
}