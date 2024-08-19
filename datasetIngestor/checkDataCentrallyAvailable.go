package datasetIngestor

import (
	"errors"
	"fmt"
	"io"
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
func CheckDataCentrallyAvailableSsh(username string, ARCHIVEServer string, sourceFolder string, sshOutput io.Writer) (sshErr error, otherErr error) {
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
		return nil, fmt.Errorf("unsupported operating system: %s", os)
	}

	// Redirect the command's output to sshOutput var
	cmd.Stdout = sshOutput
	cmd.Stderr = sshOutput

	// Run the command and return any error that occurs.
	sshErr = cmd.Run()
	return sshErr, nil
}
