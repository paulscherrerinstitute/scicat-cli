package orchestrator

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/paulscherrerinstitute/scicat-cli/v3/datasetIngestor"
)

func newTestDatasetFolder(t *testing.T, files map[string]string) string {
	t.Helper()
	tempDir, err := os.MkdirTemp("", "prepareDataset")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %s", err)
	}
	t.Cleanup(func() { os.RemoveAll(tempDir) })

	for name, content := range files {
		if err := os.WriteFile(filepath.Join(tempDir, name), []byte(content), 0644); err != nil {
			t.Fatalf("Failed to create test file %s: %s", name, err)
		}
	}
	return tempDir
}

// --- PrepareDataset ---

func TestPrepareDataset_EmptyDataset(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`[]`))
	}))
	defer ts.Close()

	folder := newTestDatasetFolder(t, nil)
	var emptyDatasets, tooLargeDatasets int

	_, err := PrepareDataset(ts.Client(), ts.URL, map[string]string{"accessToken": "testToken"},
		map[string]string{}, map[string]interface{}{"ownerGroup": datasetIngestor.DUMMY_OWNER}, 1,
		folder, "", nil, nil, &emptyDatasets, &tooLargeDatasets)

	var emptyErr *datasetIngestor.EmptyDatasetError
	if !errors.As(err, &emptyErr) {
		t.Fatalf("expected *EmptyDatasetError, got %v", err)
	}
	if emptyDatasets != 1 {
		t.Errorf("emptyDatasets = %d, want 1", emptyDatasets)
	}
	if tooLargeDatasets != 0 {
		t.Errorf("tooLargeDatasets = %d, want 0", tooLargeDatasets)
	}
}

func TestPrepareDataset_NormalDatasetUpdatesMetadata(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`[]`))
	}))
	defer ts.Close()

	folder := newTestDatasetFolder(t, map[string]string{"a": "hello"})
	metaDataMap := map[string]interface{}{"ownerGroup": datasetIngestor.DUMMY_OWNER}
	var emptyDatasets, tooLargeDatasets int

	fullFileArray, err := PrepareDataset(ts.Client(), ts.URL, map[string]string{"accessToken": "testToken"},
		map[string]string{}, metaDataMap, 1, folder, "", nil, nil, &emptyDatasets, &tooLargeDatasets)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(fullFileArray) != 1 {
		t.Errorf("expected 1 file, got %d", len(fullFileArray))
	}
	if _, ok := metaDataMap["license"]; !ok {
		t.Errorf("expected metadata to be updated with a license field")
	}
	if metaDataMap["ownerGroup"] == datasetIngestor.DUMMY_OWNER {
		t.Errorf("expected ownerGroup to no longer be the dummy value")
	}
	if emptyDatasets != 0 || tooLargeDatasets != 0 {
		t.Errorf("did not expect any dataset to be skipped")
	}
}

func TestPrepareDataset_TooManyFiles(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`[]`))
	}))
	defer ts.Close()

	folder := newTestDatasetFolder(t, map[string]string{"a": "hello"})

	// GetLocalFileList walks each line of the file listing independently, so listing the same
	// real file more times than TOTAL_MAXFILES allows drives numFiles past the limit without
	// creating hundreds of thousands of files on disk (which is prohibitively slow, especially
	// on Windows with real-time antivirus scanning).
	var listing strings.Builder
	for i := 0; i < datasetIngestor.TOTAL_MAXFILES+1; i++ {
		listing.WriteString("a\n")
	}
	listingPath := filepath.Join(folder, "filelisting.txt")
	if err := os.WriteFile(listingPath, []byte(listing.String()), 0644); err != nil {
		t.Fatalf("failed to write file listing: %s", err)
	}
	var emptyDatasets, tooLargeDatasets int

	_, err := PrepareDataset(ts.Client(), ts.URL, map[string]string{"accessToken": "testToken"},
		map[string]string{}, map[string]interface{}{"ownerGroup": datasetIngestor.DUMMY_OWNER}, 1,
		folder, listingPath, nil, nil, &emptyDatasets, &tooLargeDatasets)

	var tooManyErr *datasetIngestor.TooManyFilesError
	if !errors.As(err, &tooManyErr) {
		t.Fatalf("expected *TooManyFilesError, got %v", err)
	}
	if tooLargeDatasets != 1 {
		t.Errorf("tooLargeDatasets = %d, want 1", tooLargeDatasets)
	}
	if emptyDatasets != 0 {
		t.Errorf("emptyDatasets = %d, want 0", emptyDatasets)
	}
}

func TestPrepareDataset_GetLocalFileListError(t *testing.T) {
	var emptyDatasets, tooLargeDatasets int

	_, err := PrepareDataset(http.DefaultClient, "", map[string]string{}, map[string]string{}, map[string]interface{}{}, 1,
		filepath.Join(os.TempDir(), "does-not-exist-prepareDataset"), "", nil, nil, &emptyDatasets, &tooLargeDatasets)
	if err == nil {
		t.Fatal("expected an error for a nonexistent source folder, got nil")
	}
	var emptyErr *datasetIngestor.EmptyDatasetError
	if errors.As(err, &emptyErr) {
		t.Errorf("did not expect an *EmptyDatasetError for a scan failure, got %v", err)
	}
	if emptyDatasets != 0 || tooLargeDatasets != 0 {
		t.Errorf("did not expect any counter to be incremented for a scan failure")
	}
}

// --- UpdateAndLogMetaData ---

func TestUpdateAndLogMetaData(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`[]`))
	}))
	defer ts.Close()

	client := ts.Client()
	user := map[string]string{"accessToken": "testToken"}
	originalMap := map[string]string{}
	metaDataMap := map[string]interface{}{"ownerGroup": datasetIngestor.DUMMY_OWNER}

	startTime := time.Now()
	endTime := startTime.Add(time.Hour)
	UpdateAndLogMetaData(client, ts.URL, user, originalMap, metaDataMap, startTime, endTime, "testOwner", 1)

	if _, ok := metaDataMap["license"]; !ok {
		t.Errorf("expected metadata to be updated with a license field")
	}
	if metaDataMap["ownerGroup"] != "testOwner" {
		t.Errorf("expected ownerGroup to be updated to 'testOwner', got %v", metaDataMap["ownerGroup"])
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

	copyFlag, err := ResolveCentralAvailability("user", "server", "/some/folder", nil, false, []string{"group1"}, false, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if copyFlag {
		t.Errorf("did not expect copyFlag to be set when data is centrally available")
	}
}

func TestResolveCentralAvailability_Available_PreservesCurrentCopyFlag(t *testing.T) {
	withSshMocks(t, nil, false)

	copyFlag, err := ResolveCentralAvailability("user", "server", "/some/folder", nil, true, []string{"group1"}, false, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !copyFlag {
		t.Errorf("expected copyFlag to remain true when data is centrally available")
	}
}

func TestResolveCentralAvailability_NotAvailable_Noninteractive(t *testing.T) {
	withSshMocks(t, errors.New("not found"), true)

	copyFlag, err := ResolveCentralAvailability("user", "server", "/some/folder", nil, false, []string{"group1"}, true, nil)
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

	_, err := ResolveCentralAvailability("user", "server", "/some/folder", nil, false, nil, false, func() bool { return true })
	if !errors.Is(err, ErrCopyRequiresPersonalAccount) {
		t.Fatalf("expected ErrCopyRequiresPersonalAccount, got %v", err)
	}
}

func TestResolveCentralAvailability_UserAborts(t *testing.T) {
	withSshMocks(t, errors.New("not found"), true)

	_, err := ResolveCentralAvailability("user", "server", "/some/folder", nil, false, []string{"group1"}, false, func() bool { return false })
	if !errors.Is(err, ErrIngestAborted) {
		t.Fatalf("expected ErrIngestAborted, got %v", err)
	}
}

func TestResolveCentralAvailability_UserConfirms(t *testing.T) {
	withSshMocks(t, errors.New("not found"), true)

	copyFlag, err := ResolveCentralAvailability("user", "server", "/some/folder", nil, false, []string{"group1"}, false, func() bool { return true })
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

	_, err := ResolveCentralAvailability("user", "server", "/some/folder", nil, false, []string{"group1"}, false, nil)
	var warning *NotCentrallyAvailableWarning
	if err == nil || errors.As(err, &warning) {
		t.Fatalf("expected a plain error, got %v", err)
	}
}
