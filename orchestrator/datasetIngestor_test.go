package orchestrator

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/paulscherrerinstitute/scicat-cli/v3/datasetIngestor"
)

// --- PrepareDataset ---

func TestPrepareDataset(t *testing.T) {
	tests := []struct {
		name              string
		fileListErr       error
		checkErr          func(t *testing.T, err error)
		wantEmptyDatasets int
		wantTooLarge      int
	}{
		{
			name: "success updates metadata and returns the file list",
			checkErr: func(t *testing.T, err error) {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			},
		},
		{
			name:        "empty dataset increments emptyDatasets",
			fileListErr: &datasetIngestor.EmptyDatasetError{SourceFolder: "/some/folder"},
			checkErr: func(t *testing.T, err error) {
				var emptyErr *datasetIngestor.EmptyDatasetError
				if !errors.As(err, &emptyErr) {
					t.Fatalf("expected *EmptyDatasetError, got %v", err)
				}
			},
			wantEmptyDatasets: 1,
		},
		{
			name:        "too many files increments tooLargeDatasets",
			fileListErr: &datasetIngestor.TooManyFilesError{SourceFolder: "/some/folder", NumFiles: 500000, MaxFiles: 400000},
			checkErr: func(t *testing.T, err error) {
				var tooManyErr *datasetIngestor.TooManyFilesError
				if !errors.As(err, &tooManyErr) {
					t.Fatalf("expected *TooManyFilesError, got %v", err)
				}
			},
			wantTooLarge: 1,
		},
		{
			name:        "other error is not categorized as empty or too-large",
			fileListErr: errors.New("something else went wrong"),
			checkErr: func(t *testing.T, err error) {
				if err == nil {
					t.Fatal("expected an error, got nil")
				}
				var emptyErr *datasetIngestor.EmptyDatasetError
				var tooManyErr *datasetIngestor.TooManyFilesError
				if errors.As(err, &emptyErr) || errors.As(err, &tooManyErr) {
					t.Fatalf("did not expect a categorized error, got %v (%T)", err, err)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			oldList := getValidatedLocalFileListFunc
			oldUpdate := updateMetadataFunc
			t.Cleanup(func() {
				getValidatedLocalFileListFunc = oldList
				updateMetadataFunc = oldUpdate
			})

			wantFiles := []datasetIngestor.Datafile{{Path: "a"}}
			getValidatedLocalFileListFunc = func(sourceFolder string, filelistingPath string,
				symlinkCallback func(symlinkPath string, sourceFolder string) (bool, error),
				filenameFilterCallback func(filepath string) bool,
			) ([]datasetIngestor.Datafile, time.Time, time.Time, string, int64, int64, error) {
				if tt.fileListErr != nil {
					return nil, time.Time{}, time.Time{}, "", 0, 0, tt.fileListErr
				}
				return wantFiles, time.Now(), time.Now(), "abc", 1, 10, nil
			}

			updateMetadataCalled := false
			updateMetadataFunc = func(client *http.Client, APIServer string, user map[string]string,
				originalMap map[string]string, metaDataMap map[string]interface{}, startTime time.Time, endTime time.Time, owner string, tapecopies int) {
				updateMetadataCalled = true
			}

			var emptyDatasets, tooLargeDatasets int
			fullFileArray, err := PrepareDatasetAndUpdateCounts(nil, "", map[string]string{"accessToken": "testToken"},
				map[string]string{}, map[string]interface{}{"ownerGroup": datasetIngestor.DUMMY_OWNER}, 1,
				"/some/folder", "", nil, nil, &emptyDatasets, &tooLargeDatasets)

			tt.checkErr(t, err)

			wantCalled := tt.fileListErr == nil
			if updateMetadataCalled != wantCalled {
				t.Errorf("updateMetadataFunc called = %v, want %v", updateMetadataCalled, wantCalled)
			}
			if wantCalled && len(fullFileArray) != len(wantFiles) {
				t.Errorf("expected %d file(s), got %d", len(wantFiles), len(fullFileArray))
			}
			if emptyDatasets != tt.wantEmptyDatasets {
				t.Errorf("emptyDatasets = %d, want %d", emptyDatasets, tt.wantEmptyDatasets)
			}
			if tooLargeDatasets != tt.wantTooLarge {
				t.Errorf("tooLargeDatasets = %d, want %d", tooLargeDatasets, tt.wantTooLarge)
			}
		})
	}
}

// --- PrepareRemoteDataset ---

func TestPrepareRemoteDataset(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`[]`))
	}))
	defer ts.Close()

	client := ts.Client()
	user := map[string]string{"accessToken": "testToken"}
	originalMap := map[string]string{}
	metaDataMap := map[string]interface{}{
		"ownerGroup":   datasetIngestor.DUMMY_OWNER,
		"owner":        "testOwner",
		"creationTime": datasetIngestor.DUMMY_TIME,
	}

	before := time.Now()
	PrepareRemoteDataset(client, ts.URL, user, originalMap, metaDataMap, 1)
	after := time.Now()

	if _, ok := metaDataMap["license"]; !ok {
		t.Errorf("expected metadata to be updated with a license field")
	}
	if metaDataMap["ownerGroup"] != "testOwner" {
		t.Errorf("expected ownerGroup to be updated to 'testOwner', got %v", metaDataMap["ownerGroup"])
	}
	creationTime, ok := metaDataMap["creationTime"].(time.Time)
	if !ok {
		t.Fatalf("expected creationTime to be set to a time.Time, got %v", metaDataMap["creationTime"])
	}
	if creationTime.Before(before) || creationTime.After(after) {
		t.Errorf("expected creationTime to be set to roughly now, got %v (want between %v and %v)", creationTime, before, after)
	}
}

// --- DetermineDatasetLifecycle ---

func TestDetermineDatasetLifecycle(t *testing.T) {
	tests := []struct {
		name                     string
		copyFlag                 bool
		remoteFilesFlag          bool
		wantArchivable           bool
		wantMetaArchivable       bool
		wantIsOnCentralDisk      bool
		wantArchiveStatusMessage string
	}{
		{"local, no copy needed", false, false, true, true, true, "datasetCreated"},
		{"local, copy needed", true, false, false, false, false, "filesNotYetAvailable"},
		// remoteFilesFlag forces the metadata's archivable field to false (the origin datablocks
		// aren't registered yet), but the CLI still queues an archive job (archivable stays true)
		// since there's no later CLI-driven step, unlike the copyFlag case, that would flip it.
		{"remote files", false, true, true, false, true, "origDatablocksNotYetAvailable"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			archivable, metaArchivable, isOnCentralDisk, archiveStatusMessage := DetermineDatasetLifecycle(tt.copyFlag, tt.remoteFilesFlag)
			if archivable != tt.wantArchivable {
				t.Errorf("archivable = %v, want %v", archivable, tt.wantArchivable)
			}
			if metaArchivable != tt.wantMetaArchivable {
				t.Errorf("metaArchivable = %v, want %v", metaArchivable, tt.wantMetaArchivable)
			}
			if isOnCentralDisk != tt.wantIsOnCentralDisk {
				t.Errorf("isOnCentralDisk = %v, want %v", isOnCentralDisk, tt.wantIsOnCentralDisk)
			}
			if archiveStatusMessage != tt.wantArchiveStatusMessage {
				t.Errorf("archiveStatusMessage = %v, want %v", archiveStatusMessage, tt.wantArchiveStatusMessage)
			}
		})
	}
}

// --- ResolveCentralAvailability ---

func withSshMocks(t *testing.T, checkErr error, notFound bool) {
	t.Helper()
	oldGoos := datasetIngestor.Goos
	oldNewDumbClient := datasetIngestor.NewDumbClientFunc
	oldCheckRemoteDirectory := datasetIngestor.CheckRemoteDirectoryFunc
	oldIsNotFound := datasetIngestor.IsRemoteDirectoryNotFound
	t.Cleanup(func() {
		datasetIngestor.Goos = oldGoos
		datasetIngestor.NewDumbClientFunc = oldNewDumbClient
		datasetIngestor.CheckRemoteDirectoryFunc = oldCheckRemoteDirectory
		datasetIngestor.IsRemoteDirectoryNotFound = oldIsNotFound
	})
	// Force the pure-Go SSH client path (normally Windows-only) so the mocks below take effect
	// regardless of the OS running the tests.
	datasetIngestor.Goos = "windows"
	datasetIngestor.NewDumbClientFunc = func(username, password, server string) (*datasetIngestor.Client, error) {
		return &datasetIngestor.Client{}, nil
	}
	datasetIngestor.CheckRemoteDirectoryFunc = func(c *datasetIngestor.Client, sourceFolder string, sshOutput io.Writer) error {
		return checkErr
	}
	datasetIngestor.IsRemoteDirectoryNotFound = func(err error) bool { return notFound }
}

func TestResolveCentralAvailability_Available(t *testing.T) {
	withSshMocks(t, nil, false)

	copyFlag, err := ResolveCentralAvailability("user", "server", "/some/folder", false, []string{"group1"}, false, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if copyFlag {
		t.Errorf("did not expect copyFlag to be set when data is centrally available")
	}
}

func TestResolveCentralAvailability_Available_PreservesCurrentCopyFlag(t *testing.T) {
	withSshMocks(t, nil, false)

	copyFlag, err := ResolveCentralAvailability("user", "server", "/some/folder", true, []string{"group1"}, false, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !copyFlag {
		t.Errorf("expected copyFlag to remain true when data is centrally available")
	}
}

func TestResolveCentralAvailability_NotAvailable_Noninteractive(t *testing.T) {
	withSshMocks(t, errors.New("not found"), true)

	copyFlag, err := ResolveCentralAvailability("user", "server", "/some/folder", false, []string{"group1"}, true, nil)
	var warning *NotCentrallyAvailableWarning
	if !errors.As(err, &warning) {
		t.Fatalf("expected a *NotCentrallyAvailableWarning, got %v", err)
	}
	if !copyFlag {
		t.Errorf("expected copyFlag to be true when data is not centrally available")
	}
}

func TestResolveCentralAvailability_NoAccessGroups(t *testing.T) {
	withSshMocks(t, errors.New("not found"), true)

	_, err := ResolveCentralAvailability("user", "server", "/some/folder", false, nil, false, func() bool { return true })
	if !errors.Is(err, ErrCopyRequiresPersonalAccount) {
		t.Fatalf("expected ErrCopyRequiresPersonalAccount, got %v", err)
	}
}

func TestResolveCentralAvailability_UserAborts(t *testing.T) {
	withSshMocks(t, errors.New("not found"), true)

	_, err := ResolveCentralAvailability("user", "server", "/some/folder", false, []string{"group1"}, false, func() bool { return false })
	if !errors.Is(err, ErrIngestAborted) {
		t.Fatalf("expected ErrIngestAborted, got %v", err)
	}
}

func TestResolveCentralAvailability_UserConfirms(t *testing.T) {
	withSshMocks(t, errors.New("not found"), true)

	copyFlag, err := ResolveCentralAvailability("user", "server", "/some/folder", false, []string{"group1"}, false, func() bool { return true })
	var warning *NotCentrallyAvailableWarning
	if !errors.As(err, &warning) {
		t.Fatalf("expected a *NotCentrallyAvailableWarning, got %v", err)
	}
	if !copyFlag {
		t.Errorf("expected copyFlag to be true")
	}
}

func TestResolveCentralAvailability_OtherError(t *testing.T) {
	oldGoos := datasetIngestor.Goos
	oldNewDumbClient := datasetIngestor.NewDumbClientFunc
	t.Cleanup(func() {
		datasetIngestor.Goos = oldGoos
		datasetIngestor.NewDumbClientFunc = oldNewDumbClient
	})
	datasetIngestor.Goos = "windows"
	datasetIngestor.NewDumbClientFunc = func(username, password, server string) (*datasetIngestor.Client, error) {
		return nil, errors.New("connection refused")
	}

	_, err := ResolveCentralAvailability("user", "server", "/some/folder", false, []string{"group1"}, false, nil)
	var warning *NotCentrallyAvailableWarning
	if err == nil || errors.As(err, &warning) {
		t.Fatalf("expected a plain error, got %v", err)
	}
}
