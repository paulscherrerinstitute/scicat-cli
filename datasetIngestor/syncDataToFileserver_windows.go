// functionality needed for "de-central" data
package datasetIngestor

import (
	"fmt"
	"io"
	"os/exec"
	"path/filepath"
	"strings"
)

// buildDestPaths computes the UNC share root and destination folder.
func buildDestPaths(datasetId, RSYNCServer, sourceFolder string) (shareRoot, destFolder string) {
	parts := strings.Split(datasetId, "/")
	shortDatasetId := "unknown"
	if len(parts) > 1 {
		shortDatasetId = parts[1]
	}

	// Strip port number (e.g. "server:22") — UNC paths don't use ports
	server := strings.Split(RSYNCServer, ":")[0]

	// Remove leading drive letter (e.g. "C:") if present
	ss := strings.Split(sourceFolder, ":")
	destPath := filepath.ToSlash(ss[len(ss)-1])

	shareRoot = `\\` + server + `\archive`
	destFolder = filepath.Join(shareRoot, shortDatasetId, destPath)

	// Ensure separators match Windows style for the final command
	return filepath.Clean(shareRoot), filepath.Clean(destFolder)
}

// buildNetUseArgs constructs the argument list for "net use".
func buildNetUseArgs(share, username, password string) []string {
	args := []string{"use", share}
	if password != "" {
		args = append(args, password)
	}
	args = append(args, "/user:"+username)
	return args
}

// SyncLocalDataToFileserver handles data transfer using native Windows tools.
func SyncLocalDataToFileserver(datasetId string, user map[string]string, RSYNCServer string, sourceFolder string, absFileListing string, commandOutput io.Writer) error {
	username := user["username"]
	password := user["password"]
	shareRoot, destFolder := buildDestPaths(datasetId, RSYNCServer, sourceFolder)

	// 1. Establish authenticated SMB session
	if err := netUseConnect(commandOutput, shareRoot, username, password); err != nil {
		return fmt.Errorf("failed to connect to share %s: %v", shareRoot, err)
	}
	defer netUseDisconnect(commandOutput, shareRoot)

	// 2. Perform file transfer
	if absFileListing != "" {
		lines, err := readLines(absFileListing)
		if err != nil {
			return fmt.Errorf("could not read filelist: %v", err)
		}
		for _, line := range lines {
			srcDir := filepath.Join(sourceFolder, filepath.Dir(line))
			destDir := filepath.Join(destFolder, filepath.Dir(line))
			fileName := filepath.Base(line)

			fmt.Fprintf(commandOutput, "Copying: %s\n", line)
			if err := runRobocopy(commandOutput, srcDir, destDir, fileName); err != nil {
				return fmt.Errorf("robocopy failed on %s: %v", line, err)
			}
		}
	} else {
		fmt.Fprintf(commandOutput, "Copying directory: %s\n", sourceFolder)
		if err := runRobocopy(commandOutput, sourceFolder, destFolder, "/E"); err != nil {
			return fmt.Errorf("robocopy failed: %v", err)
		}
	}
	return nil
}

func runRobocopy(output io.Writer, src, dest string, extraArgs ...string) error {
	args := append([]string{src, dest}, extraArgs...)
	args = append(args, "/COPY:DAT", "/DCOPY:T", "/R:3", "/W:5", "/NP")

	cmd := exec.Command("robocopy", args...)
	cmd.Stdout = output
	cmd.Stderr = output

	err := cmd.Run()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok && exitErr.ExitCode() < 8 {
			return nil
		}
		return err
	}
	return nil
}

func netUseConnect(output io.Writer, share, username, password string) error {
	args := buildNetUseArgs(share, username, password)
	cmd := exec.Command("net", args...)
	cmd.Stdout = output
	cmd.Stderr = output
	fmt.Fprintf(output, "Connecting to %s as %s...\n", share, username)
	return cmd.Run()
}

func netUseDisconnect(output io.Writer, share string) {
	err := exec.Command("net", "use", share, "/delete", "/yes").Run()
	if err != nil {
		fmt.Fprintf(output, "Cleanup warning: %v\n", err)
	}
}
