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
)

type RsyncCmd struct {
	Path        string
	Version     string
	StderrFlags []string
}

// functionality needed for "de-central" data
// copies data from a local machine to a fileserver, uses RSync underneath
func SyncLocalDataToFileserver(datasetId string, user map[string]string, RSYNCServer string, sourceFolder string, absFileListing string, cmdOutput io.Writer) (err error) {
	username := user["username"]
	shortDatasetId := strings.Split(datasetId, "/")[1]
	destFolder := "archive/" + shortDatasetId + sourceFolder
	serverConnectString := fmt.Sprintf("%s@%s:%s", username, RSYNCServer, destFolder)
	// append trailing slash to sourceFolder to indicate that the *contents* of the folder should be copied
	// no special handling for blanks in sourceFolder needed here
	fullSourceFolderPath := sourceFolder + "/"

	rsyncCmd, err := getRsyncCmd()
	if err != nil {
		return err
	}

	cmd := buildRsyncCmd(rsyncCmd, absFileListing, fullSourceFolderPath, serverConnectString)

	// Show rsync's output
	cmd.Stdout = cmdOutput
	cmd.Stderr = cmdOutput
	fmt.Fprintf(cmdOutput, "Running: %v.\n", cmd.Args)
	err = cmd.Run()
	return err
}

// Inspect the installed rsync binary
func getRsyncCmd() (*RsyncCmd, error) {
	path := "/usr/bin/rsync"
	versionNumber, err := getRsyncVersion(path)
	if err != nil {
		return nil, err
	}
	stderrFlags, err := detectRsyncStderrSupport(path)
	if err != nil {
		return nil, err
	}
	return &RsyncCmd{
		path, versionNumber, stderrFlags,
	}, nil
}

// Get rsync version
func getRsyncVersion(rsyncPath string) (string, error) {
	cmd := exec.Command(rsyncPath, "--version")
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

// Detects if rsync supports --stderr and/or --msgs2stderr by parsing the output of rsync --help.
// Returns two booleans: (supportsStderr, supportsMsgs2stderr)
func detectRsyncStderrSupport(rsyncPath string) ([]string, error) {
	cmd := exec.Command(rsyncPath, "--help")
	output, err := cmd.Output()
	if err != nil {
		return []string{}, fmt.Errorf("error running /usr/bin/rsync --help: %v", err)
	}
	helpText := string(output)
	if strings.Contains(helpText, "--stderr") {
		return []string{"--stderr=error"}, nil
	}
	if strings.Contains(helpText, "--msgs2stderr") {
		return []string{"-q", "--msgs2stderr"}, nil
	}
	return []string{}, nil
}

// Check rsync version and adjust command accordingly
func buildRsyncCmd(rsyncCmd *RsyncCmd, absFileListing, fullSourceFolderPath, serverConnectString string) *exec.Cmd {
	rsyncFlags := []string{"-e", "ssh", "-avx", "--progress"}
	if absFileListing != "" {
		rsyncFlags = append([]string{"-r", "--files-from", absFileListing}, rsyncFlags...)
	}
	rsyncFlags = append(rsyncFlags, rsyncCmd.StderrFlags...)

	cmd := exec.Command(rsyncCmd.Path, append(rsyncFlags, fullSourceFolderPath, serverConnectString)...)
	return cmd
}
