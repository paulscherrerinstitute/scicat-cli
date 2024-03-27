package datasetIngestor

import (
    "os/exec"
    "reflect"
    "testing"
)

// Mock for exec.Command
type execCommandMock struct {
	expectedArgs []string
	returnError  error
}

func (m *execCommandMock) Command(name string, arg ...string) *exec.Cmd {
	if name != "/usr/bin/ssh" || !reflect.DeepEqual(arg, m.expectedArgs) {
		panic("unexpected arguments")
	}
	
	return exec.Command("echo", "mocked")
}

func TestTestDataCentrallyAvailable(t *testing.T) {
	tests := []struct {
		name         string
		username     string
		archiveServer string
		sourceFolder string
		wantErr      bool
		}{
			{
				name:         "test data centrally available",
				username:     "testuser",
				archiveServer: "testserver",
				sourceFolder: "/test/folder",
				wantErr:      false,
			},
			// Add more test cases here.
	}
	
	
	for _, tt := range tests {
		oldExecCommand := execCommand
		execCommand = (&execCommandMock{
			expectedArgs: []string{"-q", "-l", tt.username, tt.archiveServer, "test", "-d", tt.sourceFolder},
			returnError:  nil,
			}).Command
			defer func() { execCommand = oldExecCommand }()
		t.Run(tt.name, func(t *testing.T) {
			// Replace exec.Command with a mock
			oldExecCommand := execCommand
			execCommand = (&execCommandMock{
				expectedArgs: []string{"-q", "-l", tt.username, tt.archiveServer, "test", "-d", tt.sourceFolder},
				returnError:  nil,
				}).Command
				defer func() { execCommand = oldExecCommand }()
				
				err := TestDataCentrallyAvailable(tt.username, tt.archiveServer, tt.sourceFolder)
				if (err != nil) != tt.wantErr {
					t.Errorf("TestDataCentrallyAvailable() error = %v, wantErr %v", err, tt.wantErr)
				}
		})
	}
}
