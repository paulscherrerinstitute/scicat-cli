package datasetIngestor

import (
	"errors"
	"log"
	"os"
	"os/exec"
	"runtime"
)

// execCommand is a variable that points to exec.Command, allowing it to be replaced in tests.
var execCommand = exec.Command

// CheckDataCentrallyAvailable checks if a specific directory (sourceFolder) is available on a remote server (ARCHIVEServer)
// using the provided username for SSH connection. It returns an error if the directory is not available or if there's an issue with the SSH connection.
func CheckDataCentrallyAvailable(username string, ARCHIVEServer string, sourceFolder string) (err error) {
	var cmd *exec.Cmd

	// Check the operating system
	switch os := runtime.GOOS; os {
	case "linux", "windows", "darwin":
		// Check if ssh exists
		_, err := exec.LookPath("ssh") // locate a program in the user's path
		if err != nil {
			log.Println("SSH is not installed. Please install OpenSSH client.")
			return err
		}

		// Create a new exec.Command to run the SSH command. The command checks if the directory exists on the remote server.
		// The "-q" option suppresses all warnings, "-l" specifies the login name on the remote server.
		cmd = execCommand("ssh", "-q", "-l", username, ARCHIVEServer, "test", "-d", sourceFolder)
	default:
		log.Printf("%s is not supported.\n", os)
		return errors.New("unsupported operating system")
	}

	// Redirect the command's standard error to the process's standard error.
	// This means that any error messages from the command will be displayed in the terminal.
	cmd.Stderr = os.Stderr

	// Log the command that is being run for debugging purposes.
	log.Printf("Running %v.\n", cmd.Args)

	// Run the command and return any error that occurs.
	err = cmd.Run()
	return err
}
