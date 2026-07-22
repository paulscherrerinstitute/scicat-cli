package datasetIngestor

import (
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"reflect"
	"strconv"
	"testing"
)

// execCommandMock replaces execCommand with one that re-execs this test binary (via
// TestHelperProcess below) instead of actually running ssh, so tests can control the exit code
// without a real ssh binary or network access.
type execCommandMock struct {
	t            *testing.T
	expectedArgs []string
	exitStatus   int
}

func (m *execCommandMock) Command(name string, arg ...string) *exec.Cmd {
	m.t.Helper()
	if !reflect.DeepEqual(arg, m.expectedArgs) {
		m.t.Errorf("unexpected arguments: got %v, want %v", arg, m.expectedArgs)
	}

	cs := make([]string, 0, 3+len(arg))
	cs = append(cs, "-test.run=TestHelperProcess", "--", name)
	cs = append(cs, arg...)
	cmd := exec.Command(os.Args[0], cs...)
	cmd.Env = []string{"GO_WANT_HELPER_PROCESS=1", fmt.Sprintf("EXIT_STATUS=%d", m.exitStatus)}
	return cmd
}

// TestHelperProcess isn't a real test. It's a helper subprocess used by execCommandMock.
func TestHelperProcess(t *testing.T) {
	if os.Getenv("GO_WANT_HELPER_PROCESS") != "1" {
		return
	}
	fmt.Fprint(os.Stdout, "output")
	fmt.Fprint(os.Stderr, "error")
	exitStatus, _ := strconv.Atoi(os.Getenv("EXIT_STATUS"))
	os.Exit(exitStatus)
}

func TestCheckDataCentrallyAvailable(t *testing.T) {
	tests := []struct {
		name          string
		username      string
		archiveServer string
		sourceFolder  string
		expectedArgs  []string
		exitStatus    int
		wantSshErr    bool
		wantOtherErr  bool
	}{
		{
			name:          "data centrally available",
			username:      "testuser",
			archiveServer: "testserver",
			sourceFolder:  "/test/folder",
			expectedArgs:  []string{"-q", "-l", "testuser", "testserver", "test", "-d", "/test/folder"},
			exitStatus:    0,
		},
		{
			name:          "data not available",
			username:      "testuser",
			archiveServer: "testserver",
			sourceFolder:  "/nonexistent/folder",
			expectedArgs:  []string{"-q", "-l", "testuser", "testserver", "test", "-d", "/nonexistent/folder"},
			exitStatus:    1,
			wantSshErr:    true,
		},
		{
			name:          "other ssh failure",
			username:      "testuser",
			archiveServer: "testserver",
			sourceFolder:  "/some/folder",
			expectedArgs:  []string{"-q", "-l", "testuser", "testserver", "test", "-d", "/some/folder"},
			exitStatus:    255,
			wantOtherErr:  true,
		},
		{
			name:          "server with explicit port",
			username:      "testuser",
			archiveServer: "testserver:2022",
			sourceFolder:  "/test/folder",
			expectedArgs:  []string{"-q", "-p", "2022", "-l", "testuser", "testserver", "test", "-d", "/test/folder"},
			exitStatus:    0,
		},
	}

	oldGoos := goos
	t.Cleanup(func() { goos = oldGoos })
	goos = "linux"

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			oldExecCommand := execCommand
			t.Cleanup(func() { execCommand = oldExecCommand })
			execCommand = (&execCommandMock{t: t, expectedArgs: tt.expectedArgs, exitStatus: tt.exitStatus}).Command

			sshErr, otherErr := CheckDataCentrallyAvailableSsh(tt.username, tt.archiveServer, tt.sourceFolder, nil)
			if (sshErr != nil) != tt.wantSshErr {
				t.Errorf("CheckDataCentrallyAvailableSsh() sshErr = %v, wantSshErr %v", sshErr, tt.wantSshErr)
			}
			if (otherErr != nil) != tt.wantOtherErr {
				t.Errorf("CheckDataCentrallyAvailableSsh() otherErr = %v, wantOtherErr %v", otherErr, tt.wantOtherErr)
			}
		})
	}
}

func TestCheckDataCentrallyAvailable_NoSshBinary(t *testing.T) {
	oldGoos := goos
	t.Cleanup(func() { goos = oldGoos })
	goos = "linux"

	oldPath := os.Getenv("PATH")
	t.Cleanup(func() { os.Setenv("PATH", oldPath) })
	os.Setenv("PATH", "")

	sshErr, otherErr := CheckDataCentrallyAvailableSsh("testuser", "testserver", "/test/folder", nil)
	if sshErr != nil {
		t.Errorf("expected no sshErr, got %v", sshErr)
	}
	if otherErr == nil {
		t.Fatal("expected an error when ssh is not available, got nil")
	}
}

func TestCheckDataCentrallyAvailable_Windows(t *testing.T) {
	tests := []struct {
		name          string
		isNotFoundErr bool
		wantSshErr    bool
		wantOtherErr  bool
		errMsg        string
	}{
		{
			name: "data centrally available",
		},
		{
			name:          "data not available",
			isNotFoundErr: true,
			wantSshErr:    true,
			errMsg:        "exit status 1",
		},
		{
			name:         "other ssh failure",
			wantOtherErr: true,
			errMsg:       "ssh transport failure",
		},
	}

	oldGoos := goos
	oldNewDumbClient := newDumbClient
	oldCheckRemoteDirectory := checkRemoteDirectory
	oldIsRemoteDirectoryNotFound := isRemoteDirectoryNotFoundSsh
	t.Cleanup(func() {
		goos = oldGoos
		newDumbClient = oldNewDumbClient
		checkRemoteDirectory = oldCheckRemoteDirectory
		isRemoteDirectoryNotFoundSsh = oldIsRemoteDirectoryNotFound
	})
	goos = "windows"
	newDumbClient = func(username, password, server string) (*Client, error) {
		return &Client{}, nil
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var returnError error
			if tt.wantSshErr || tt.wantOtherErr {
				returnError = errors.New(tt.errMsg)
			}

			checkRemoteDirectory = func(c *Client, sourceFolder string, sshOutput io.Writer) error {
				return returnError
			}
			isRemoteDirectoryNotFoundSsh = func(err error) bool {
				return tt.isNotFoundErr
			}

			sshErr, otherErr := CheckDataCentrallyAvailableSsh("testuser", "testserver", "/test/folder", nil)
			if (sshErr != nil) != tt.wantSshErr {
				t.Errorf("CheckDataCentrallyAvailableSsh() sshErr = %v, wantSshErr %v", sshErr, tt.wantSshErr)
			}
			if (otherErr != nil) != tt.wantOtherErr {
				t.Errorf("CheckDataCentrallyAvailableSsh() otherErr = %v, wantOtherErr %v", otherErr, tt.wantOtherErr)
			}
		})
	}
}

func TestCheckDataCentrallyAvailable_Windows_NewDumbClientError(t *testing.T) {
	oldGoos := goos
	oldNewDumbClient := newDumbClient
	t.Cleanup(func() {
		goos = oldGoos
		newDumbClient = oldNewDumbClient
	})
	goos = "windows"
	newDumbClient = func(username, password, server string) (*Client, error) {
		return nil, errors.New("dial failure")
	}

	sshErr, otherErr := CheckDataCentrallyAvailableSsh("testuser", "testserver", "/test/folder", nil)
	if sshErr != nil {
		t.Errorf("expected no sshErr, got %v", sshErr)
	}
	if otherErr == nil {
		t.Fatal("expected an error when the SSH client cannot be created, got nil")
	}
}
