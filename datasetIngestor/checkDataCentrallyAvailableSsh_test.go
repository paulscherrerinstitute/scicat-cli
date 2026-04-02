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
		isNotFoundErr bool
		wantSshErr    bool
		wantOtherErr  bool
		errMsg        string
	}{
		{
			name:          "test data centrally available",
			username:      "testuser",
			archiveServer: "testserver",
			sourceFolder:  "/test/folder",
			isNotFoundErr: false,
			wantSshErr:    false,
			wantOtherErr:  false,
		},
		{
			name:          "test data not available",
			username:      "testuser",
			archiveServer: "testserver",
			sourceFolder:  "/nonexistent/folder",
			isNotFoundErr: true,
			wantSshErr:    true,
			wantOtherErr:  false,
			errMsg:        "exit status 1",
		},
		{
			name:          "test remote check other failure",
			username:      "testuser",
			archiveServer: "testserver",
			sourceFolder:  "/some/folder",
			isNotFoundErr: false,
			wantSshErr:    false,
			wantOtherErr:  true,
			errMsg:        "ssh transport failure",
		},
		// Add more test cases here.
	}

	oldNewDumbClient := newDumbClient
	oldCheckRemoteDirectory := checkRemoteDirectory
	oldIsRemoteDirectoryNotFound := isRemoteDirectoryNotFound
	defer func() {
		newDumbClient = oldNewDumbClient
		checkRemoteDirectory = oldCheckRemoteDirectory
		isRemoteDirectoryNotFound = oldIsRemoteDirectoryNotFound
	}()

	for _, tt := range tests {
		var returnError error
		if tt.wantSshErr || tt.wantOtherErr {
			returnError = errors.New(tt.errMsg)
		}

		newDumbClient = func(username, password, server string) (*Client, error) {
			return &Client{}, nil
		}
		checkRemoteDirectory = func(c *Client, sourceFolder string, sshOutput io.Writer) error {
			return returnError
		}
		isRemoteDirectoryNotFound = func(err error) bool {
			return tt.isNotFoundErr
		}

		t.Run(tt.name, func(t *testing.T) {
			sshErr, otherErr := CheckDataCentrallyAvailableSsh(tt.username, tt.archiveServer, tt.sourceFolder, nil)
			if (sshErr != nil) != tt.wantSshErr {
				t.Errorf("CheckDataCentrallyAvailable() sshErr = %v, wantSshErr %v", sshErr != nil, tt.wantSshErr)
			}
			if (otherErr != nil) != tt.wantOtherErr {
				t.Errorf("CheckDataCentrallyAvailable() otherErr = %v, wantOtherErr %v", otherErr != nil, tt.wantOtherErr)
			}
			if sshErr != nil && tt.wantSshErr && sshErr.Error() != tt.errMsg {
				t.Errorf("CheckDataCentrallyAvailable() errMsg = %v, wantErrMsg %v", sshErr.Error(), tt.errMsg)
			}
			if otherErr != nil && tt.wantOtherErr && otherErr.Error() != tt.errMsg {
				t.Errorf("CheckDataCentrallyAvailable() otherErrMsg = %v, wantErrMsg %v", otherErr.Error(), tt.errMsg)
			}
		})
	}
}
