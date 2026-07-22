package datasetIngestor

import (
	"errors"
	"io"
	"net"
	"os/exec"
	"runtime"

	"golang.org/x/crypto/ssh"
)

// goos is a variable that points to runtime.GOOS, allowing it to be replaced in tests.
var goos = runtime.GOOS

// execCommand is a variable that points to exec.Command, allowing it to be replaced in tests.
var execCommand = exec.Command

// newDumbClient is a variable that points to NewDumbClient, allowing it to be replaced in tests.
var newDumbClient = NewDumbClient

// checkRemoteDirectory is a variable that points to Client.CheckRemoteDirectory, allowing it to be
// replaced in tests.
var checkRemoteDirectory = func(c *Client, sourceFolder string, sshOutput io.Writer) error {
	return c.CheckRemoteDirectory(sourceFolder, sshOutput)
}

// isRemoteDirectoryNotFoundExec classifies native ssh command errors where remote "test -d"
// evaluated to false (exit status 1), as opposed to a connection/authentication failure or another error.
var isRemoteDirectoryNotFoundExec = func(err error) bool {
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return exitErr.ExitCode() == 1
	}
	return false
}

// isRemoteDirectoryNotFoundSsh classifies pure-Go SSH client errors where remote "test -d"
// evaluated to false (exit status 1), as opposed to a connection/authentication failure or another error.
var isRemoteDirectoryNotFoundSsh = func(err error) bool {
	var exitErr *ssh.ExitError
	if errors.As(err, &exitErr) {
		return exitErr.ExitStatus() == 1
	}
	return false
}

/*
CheckDataCentrallyAvailableSsh checks if a specific directory (sourceFolder) is available on a
remote server (ARCHIVEServer) using the provided username for SSH connection.

On Windows, no native ssh/scp binary is guaranteed to be present, so this reuses the same pure-Go
SSH client (with Kerberos/GSSAPI support) already used for the Windows data-transfer path in
scp.go. Everywhere else it shells out to the system's ssh binary rather than using that pure-Go
client, so that whatever authentication the user's environment is already set up for
(Kerberos/GSSAPI via any credential cache type, ssh-agent, ~/.ssh/config host aliases, etc.) works
exactly as it would for an interactive ssh command - no additional setup should be required of the
user.

Returned values:
  - sshErr - the error returned by the remote directory check
  - otherErr - other error that prevents the check from being executed
*/
func CheckDataCentrallyAvailableSsh(username string, ARCHIVEServer string, sourceFolder string, sshOutput io.Writer) (sshErr error, otherErr error) {
	switch goos {
	case "windows":
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
		if isRemoteDirectoryNotFoundSsh(err) {
			return err, nil
		}
		return nil, err
	default:
		if _, err := exec.LookPath("ssh"); err != nil {
			return nil, errors.New("no ssh implementation is available")
		}

		host, port, err := net.SplitHostPort(ARCHIVEServer)
		if err != nil {
			host = ARCHIVEServer
			port = ""
		}

		args := []string{"-q"}
		if port != "" {
			args = append(args, "-p", port)
		}
		// "-q" suppresses all warnings, "-l" specifies the login name on the remote server.
		args = append(args, "-l", username, host, "test", "-d", sourceFolder)

		cmd := execCommand("ssh", args...)
		cmd.Stdout = sshOutput
		cmd.Stderr = sshOutput

		err = cmd.Run()
		if err == nil {
			return nil, nil
		}
		if isRemoteDirectoryNotFoundExec(err) {
			return err, nil
		}
		return nil, err
	}
}
