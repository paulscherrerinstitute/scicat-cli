package datasetIngestor

import (
	"os/exec"
	"reflect"
	"testing"
	"errors"
	"fmt"
	"os"
	"strconv"
)

// Mock for exec.Command
type execCommandMock struct {
	expectedArgs []string
	returnError  error
}

func (m *execCommandMock) Command(name string, arg ...string) *exec.Cmd {
	if !reflect.DeepEqual(arg, m.expectedArgs) {
		panic(fmt.Sprintf("unexpected arguments: got %v, want %v", arg, m.expectedArgs))
	}

	cs := []string{"-test.run=TestHelperProcess", "--", name}
	cs = append(cs, arg...)
	cmd := exec.Command(os.Args[0], cs...)
	cmd.Env = []string{"GO_WANT_HELPER_PROCESS=1"}

	if m.returnError != nil {
		cmd.Env = append(cmd.Env, "EXIT_STATUS=1")
	} else {
		cmd.Env = append(cmd.Env, "EXIT_STATUS=0")
	}

	return cmd
}

// TestHelperProcess isn't a real test. It's used as a helper process
func TestHelperProcess(t *testing.T) {
	if os.Getenv("GO_WANT_HELPER_PROCESS") != "1" {
		return
	}
	fmt.Fprintf(os.Stdout, "output")
	fmt.Fprintf(os.Stderr, "error")
	exitStatus, _ := strconv.Atoi(os.Getenv("EXIT_STATUS"))
	os.Exit(exitStatus)
}

func TestTestDataCentrallyAvailable(t *testing.T) {
	tests := []struct {
		name         string
		username     string
		archiveServer string
		sourceFolder string
		wantErr      bool
		errMsg       string
		}{
			{
				name:         "test data centrally available",
				username:     "testuser",
				archiveServer: "testserver",
				sourceFolder: "/test/folder",
				wantErr:      false,
			},
			{
				name:         "test data not available",
				username:     "testuser",
				archiveServer: "testserver",
				sourceFolder: "/nonexistent/folder",
				wantErr:      true,
				errMsg:       "exit status 1",
			},
			// Add more test cases here.
		}
		
	for _, tt := range tests {
		expectedArgs := []string{"-q", "-l", tt.username, tt.archiveServer, "test", "-d", tt.sourceFolder}
			
		var returnError error
		if tt.wantErr {
			returnError = errors.New(tt.errMsg)
		}
			
		// Replace exec.Command with a mock
		oldExecCommand := execCommand
		execCommand = (&execCommandMock{
			expectedArgs: expectedArgs,
			returnError:  returnError,
			}).Command
		defer func() { execCommand = oldExecCommand }()
		
		t.Run(tt.name, func(t *testing.T) {
			err := TestDataCentrallyAvailable(tt.username, tt.archiveServer, tt.sourceFolder)
			if (err != nil) != tt.wantErr {
				t.Errorf("TestDataCentrallyAvailable() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err != nil && tt.wantErr && err.Error() != tt.errMsg {
				t.Errorf("TestDataCentrallyAvailable() errMsg = %v, wantErrMsg %v", err.Error(), tt.errMsg)
			}
		})
	}
}
