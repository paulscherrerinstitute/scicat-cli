//go:build aix || darwin || dragonfly || freebsd || (js && wasm) || linux || nacl || netbsd || openbsd || solaris
// +build aix darwin dragonfly freebsd js,wasm linux nacl netbsd openbsd solaris

package datasetIngestor

import (
	"regexp"
	"strings"
	"testing"
)

func TestGetRsyncVersion(t *testing.T) {
	version, err := getRsyncVersion("/usr/bin/rsync")
	if err != nil {
		t.Errorf("getRsyncVersion() returned an error: %v", err)
	}
	if version == "" {
		t.Error("getRsyncVersion() returned an empty string")
	} else {
		match, _ := regexp.MatchString(`^\d{1,2}\.\d{1,2}\.\d{1,2}$`, version)
		if !match {
			t.Error("getRsyncVersion() returned wrong version string format: ", version)
		}
	}
}

func TestBuildRsyncCmd(t *testing.T) {
	tests := []struct {
		name             string
		versionNumber    string
		stderrFlags      []string
		absFileListing   string
		fullSourceFolder string
		serverConnectStr string
		expectedCmd      string
	}{
		{
			name:             "rsync version >= 3.2.3, absFileListing not empty",
			versionNumber:    "3.2.3",
			stderrFlags:      []string{"--stderr=error"},
			absFileListing:   "/path/to/file",
			fullSourceFolder: "/source/folder",
			serverConnectStr: "user@server:/dest/folder",
			expectedCmd:      "/usr/bin/rsync -r --files-from /path/to/file -e ssh -avx --progress --stderr=error /source/folder user@server:/dest/folder",
		},
		{
			name:             "rsync version < 3.2.3, absFileListing not empty",
			versionNumber:    "3.2.2",
			stderrFlags:      []string{"-q", "--msgs2stderr"},
			absFileListing:   "/path/to/file",
			fullSourceFolder: "/source/folder",
			serverConnectStr: "user@server:/dest/folder",
			expectedCmd:      "/usr/bin/rsync -r --files-from /path/to/file -e ssh -avx --progress -q --msgs2stderr /source/folder user@server:/dest/folder",
		},
		{
			name:             "rsync version >= 3.2.3, absFileListing empty",
			versionNumber:    "3.2.3",
			stderrFlags:      []string{"--stderr=error"},
			absFileListing:   "",
			fullSourceFolder: "/source/folder",
			serverConnectStr: "user@server:/dest/folder",
			expectedCmd:      "/usr/bin/rsync -e ssh -avx --progress --stderr=error /source/folder user@server:/dest/folder",
		},
		{
			name:             "rsync version < 3.2.3, absFileListing empty",
			versionNumber:    "3.2.2",
			stderrFlags:      []string{"-q", "--msgs2stderr"},
			absFileListing:   "",
			fullSourceFolder: "/source/folder",
			serverConnectStr: "user@server:/dest/folder",
			expectedCmd:      "/usr/bin/rsync -e ssh -avx --progress -q --msgs2stderr /source/folder user@server:/dest/folder",
		},
		{
			name:             "rsync version < 3.2.3, absFileListing empty",
			versionNumber:    "2.6.9",
			stderrFlags:      []string{},
			absFileListing:   "",
			fullSourceFolder: "/source/folder",
			serverConnectStr: "user@server:/dest/folder",
			expectedCmd:      "/usr/bin/rsync -e ssh -avx --progress /source/folder user@server:/dest/folder",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rsyncCmd := RsyncCmd{"/usr/bin/rsync", tt.versionNumber, tt.stderrFlags}
			cmd := buildRsyncCmd(&rsyncCmd, tt.absFileListing, tt.fullSourceFolder, tt.serverConnectStr)
			cmdStr := strings.Join(cmd.Args, " ")
			if cmdStr != tt.expectedCmd {
				t.Errorf("Expected command: %s, got: %s", tt.expectedCmd, cmdStr)
			}
		})
	}
}
