package datasetIngestor

import (
	"errors"
	"os/exec"
	"runtime"
)

// execCommand is a variable that points to exec.Command, allowing it to be replaced in tests.
var execCommand = exec.Command

// CheckDataCentrallyAvailableSsh checks if a specific directory (sourceFolder) is available on a remote server (ARCHIVEServer)
// using the provided username for SSH connection. It returns an error if the directory is not available or if there's an issue with the SSH connection.
// Returned values:
// - sshErr - the error returned by the ssh command
// - err - other error that prevents the ssh command from being executed
func CheckDataCentrallyAvailableSsh(username string, ARCHIVEServer string, sourceFolder string) (sshErr error, otherErr error) {
	// NOTE why not use crypto/ssh ???
	// NOTE even if the folder is there, not all files might be there!
	var cmd *exec.Cmd

	// Check the operating system
	switch os := runtime.GOOS; os {
	case "linux", "windows", "darwin":
		// Check if ssh exists
		_, err := exec.LookPath("ssh") // locate a program in the user's path
		if err != nil {
			return nil, errors.New("no ssh implementation is available")
		}

		// Create a new exec.Command to run the SSH command. The command checks if the directory exists on the remote server.
		// The "-q" option suppresses all warnings, "-l" specifies the login name on the remote server.
		cmd = execCommand("ssh", "-q", "-l", username, ARCHIVEServer, "test", "-d", sourceFolder)
	default:
		//log.Printf("%s is not supported.\n", os)
		return nil, errors.New("unsupported operating system")
	}

	// Redirect the command's standard error to the process's standard error.
	// This means that any error messages from the command will be displayed in the terminal.
	// Update: We don't want a library to output to the terminal.
	// cmd.Stderr = os.Stderr

	// Log the command that is being run for debugging purposes.
	//log.Printf("Running %v.\n", cmd.Args)

	// Run the command and return any error that occurs.
	sshErr = cmd.Run()
	return sshErr, nil
}
