package datasetIngestor

import (
	"errors"
	"fmt"
	"io"
	"runtime"

	"golang.org/x/crypto/ssh"
)

// newDumbClient is a variable that points to NewDumbClient, allowing it to be replaced in tests.
var newDumbClient = NewDumbClient

// checkRemoteDirectory is a variable that points to Client.CheckRemoteDirectory, allowing it to be replaced in tests.
var checkRemoteDirectory = func(c *Client, sourceFolder string, sshOutput io.Writer) error {
	return c.CheckRemoteDirectory(sourceFolder, sshOutput)
}

// isRemoteDirectoryNotFound classifies SSH command errors where remote "test -d"
// evaluated to false (exit status 1).
var isRemoteDirectoryNotFound = func(err error) bool {
	var exitErr *ssh.ExitError
	if errors.As(err, &exitErr) {
		return exitErr.ExitStatus() == 1
	}
	return false
}

// CheckDataCentrallyAvailableSsh checks if a specific directory (sourceFolder) is available on a remote server (ARCHIVEServer)
// using the provided username for SSH connection. It returns an error if the directory is not available or if there's an issue with the SSH connection.
// Returned values:
// - sshErr - the error returned by the ssh command
// - err - other error that prevents the ssh command from being executed
func CheckDataCentrallyAvailableSsh(username string, ARCHIVEServer string, sourceFolder string, sshOutput io.Writer) (sshErr error, otherErr error) {
	// NOTE why not use crypto/ssh ???
	// NOTE even if the folder is there, not all files might be there!

	// Check the operating system
	switch os := runtime.GOOS; os {
	case "linux", "windows", "darwin":
		client, err := newDumbClient(username, "", ARCHIVEServer)
		if err != nil {
			return nil, err
		}
		if client.SshClient != nil {
			defer client.SshClient.Close()
		}

		err = checkRemoteDirectory(client, sourceFolder, sshOutput)
		if err == nil {
			return nil, nil
		}
		if isRemoteDirectoryNotFound(err) {
			return err, nil
		}
		return nil, err
	default:
		return nil, fmt.Errorf("unsupported operating system: %s", os)
	}
}
