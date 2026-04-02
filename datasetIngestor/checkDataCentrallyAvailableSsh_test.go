package datasetIngestor

import (
	"errors"
	"io"
	"testing"
)

func TestCheckDataCentrallyAvailable(t *testing.T) {
	tests := []struct {
		name          string
		username      string
		archiveServer string
		sourceFolder  string
		wantErr       bool
		errMsg        string
	}{
		{
			name:          "test data centrally available",
			username:      "testuser",
			archiveServer: "testserver",
			sourceFolder:  "/test/folder",
			wantErr:       false,
		},
		{
			name:          "test data not available",
			username:      "testuser",
			archiveServer: "testserver",
			sourceFolder:  "/nonexistent/folder",
			wantErr:       true,
			errMsg:        "exit status 1",
		},
		// Add more test cases here.
	}

	oldNewDumbClient := newDumbClient
	oldCheckRemoteDirectory := checkRemoteDirectory
	defer func() {
		newDumbClient = oldNewDumbClient
		checkRemoteDirectory = oldCheckRemoteDirectory
	}()

	for _, tt := range tests {
		var returnError error
		if tt.wantErr {
			returnError = errors.New(tt.errMsg)
		}

		newDumbClient = func(username, password, server string) (*Client, error) {
			return &Client{}, nil
		}
		checkRemoteDirectory = func(c *Client, sourceFolder string, sshOutput io.Writer) error {
			return returnError
		}

		t.Run(tt.name, func(t *testing.T) {
			sshErr, otherErr := CheckDataCentrallyAvailableSsh(tt.username, tt.archiveServer, tt.sourceFolder, nil)
			if otherErr != nil {
				t.Errorf("other error encountered: %v", otherErr)
			}
			if (sshErr != nil) != tt.wantErr {
				t.Errorf("CheckDataCentrallyAvailable() error = %v, wantErr %v", sshErr != nil, tt.wantErr)
			}
			if sshErr != nil && tt.wantErr && sshErr.Error() != tt.errMsg {
				t.Errorf("CheckDataCentrallyAvailable() errMsg = %v, wantErrMsg %v", sshErr.Error(), tt.errMsg)
			}
		})
	}
}
