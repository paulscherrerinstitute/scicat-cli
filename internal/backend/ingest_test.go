package backend

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/paulscherrerinstitute/scicat-cli/v3/cmd/cliutils"
	"github.com/paulscherrerinstitute/scicat-cli/v3/datasetIngestor"
)

func testIngestServiceBase(apiServer string, client *http.Client) *TransportEngine {
	return &TransportEngine{
		Client:    client,
		APIServer: apiServer,
		UserSession: &UserSession{
			User: map[string]string{
				"accessToken": "token-123",
				"username":    "alice",
				"mail":        "alice@example.org",
			},
			AccessGroups: []string{"group-a"},
		},
	}
}

func TestNewIngestService(t *testing.T) {
	base := &TransportEngine{}
	fileSys := &FileService{}

	svc := NewIngestService(base, fileSys)

	if svc == nil {
		t.Fatalf("expected ingest service to be initialized")
	}
	if svc.Base != base {
		t.Fatalf("expected base transport to be retained")
	}
	if svc.FileSys != fileSys {
		t.Fatalf("expected file service to be retained")
	}
}

func TestApplyLifecycleProperties(t *testing.T) {
	svc := &IngestService{}

	t.Run("copy disabled", func(t *testing.T) {
		meta := map[string]interface{}{}
		svc.ApplyLifecycleProperties(meta, false)

		lifecycle, ok := meta["datasetlifecycle"].(map[string]interface{})
		if !ok {
			t.Fatalf("expected datasetlifecycle map")
		}
		if lifecycle["isOnCentralDisk"] != true {
			t.Fatalf("expected isOnCentralDisk=true")
		}
		if lifecycle["archiveStatusMessage"] != "datasetCreated" {
			t.Fatalf("expected archiveStatusMessage=datasetCreated, got %v", lifecycle["archiveStatusMessage"])
		}
		if lifecycle["archivable"] != true {
			t.Fatalf("expected archivable=true")
		}
	})

	t.Run("copy enabled", func(t *testing.T) {
		meta := map[string]interface{}{}
		svc.ApplyLifecycleProperties(meta, true)

		lifecycle := meta["datasetlifecycle"].(map[string]interface{})
		if lifecycle["isOnCentralDisk"] != false {
			t.Fatalf("expected isOnCentralDisk=false")
		}
		if lifecycle["archiveStatusMessage"] != "filesNotYetAvailable" {
			t.Fatalf("expected archiveStatusMessage=filesNotYetAvailable, got %v", lifecycle["archiveStatusMessage"])
		}
		if lifecycle["archivable"] != false {
			t.Fatalf("expected archivable=false")
		}
	})
}

func TestExecuteDataTransferBuildsTransferParams(t *testing.T) {
	base := testIngestServiceBase("https://api.example.org", &http.Client{})

	fs := &FileService{
		GlobusConfig: cliutils.GlobusConfig{
			SourceCollection:      "src-col",
			SourcePrefixPath:      "/src",
			DestinationCollection: "dst-col",
			DestinationPrefixPath: "/dst",
		},
	}

	var captured cliutils.TransferParams
	fs.TransferFiles = func(params cliutils.TransferParams) (bool, error) {
		captured = params
		return true, nil
	}

	svc := &IngestService{Base: base, FileSys: fs}
	batch := &DatasetBatch{AbsFileListing: "/tmp/files.txt"}
	files := []datasetIngestor.Datafile{
		{Path: "a/file1", IsSymlink: false},
		{Path: "b/link", IsSymlink: true},
	}

	archivable := svc.ExecuteDataTransfer(batch, "pid-1", "/beamline/run-1", files, DatasetIngestRuntimeConfig{RSYNCServer: "rsync.psi.ch"})
	if !archivable {
		t.Fatalf("expected archivable=true from transfer callback")
	}

	if captured.DatasetId != "pid-1" {
		t.Fatalf("unexpected dataset id in transfer params: %s", captured.DatasetId)
	}
	if captured.DatasetSourceFolder != "/beamline/run-1" {
		t.Fatalf("unexpected dataset source folder: %s", captured.DatasetSourceFolder)
	}
	if captured.SshParams.ApiServer != "https://api.example.org" {
		t.Fatalf("unexpected ssh api server: %s", captured.SshParams.ApiServer)
	}
	if captured.SshParams.RsyncServer != "rsync.psi.ch" {
		t.Fatalf("unexpected ssh rsync server: %s", captured.SshParams.RsyncServer)
	}
	if captured.SshParams.AbsFilelistPath != "/tmp/files.txt" {
		t.Fatalf("unexpected abs file listing path: %s", captured.SshParams.AbsFilelistPath)
	}
	if len(captured.GlobusParams.Filelist) != 2 || captured.GlobusParams.Filelist[0] != "a/file1" || captured.GlobusParams.Filelist[1] != "b/link" {
		t.Fatalf("unexpected globus file list: %#v", captured.GlobusParams.Filelist)
	}
	if len(captured.GlobusParams.IsSymlinkList) != 2 || captured.GlobusParams.IsSymlinkList[0] || !captured.GlobusParams.IsSymlinkList[1] {
		t.Fatalf("unexpected globus symlink list: %#v", captured.GlobusParams.IsSymlinkList)
	}
}

func TestExecuteDataTransferReturnsFalseOnTransferError(t *testing.T) {
	base := testIngestServiceBase("https://api.example.org", &http.Client{})
	fs := &FileService{}
	fs.TransferFiles = func(params cliutils.TransferParams) (bool, error) {
		return false, fmt.Errorf("transfer failure")
	}

	svc := &IngestService{Base: base, FileSys: fs}
	archivable := svc.ExecuteDataTransfer(&DatasetBatch{}, "pid", "/source", nil, DatasetIngestRuntimeConfig{})
	if archivable {
		t.Fatalf("expected archivable=false when transfer returns an error")
	}
}

func TestRegisterDatasetRecordSuccess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/datasets" {
			t.Fatalf("expected /datasets path, got %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"pid":"pid-123"}`))
	}))
	defer server.Close()

	svc := &IngestService{Base: testIngestServiceBase(server.URL, server.Client())}

	datasetId, err := svc.RegisterDatasetRecord(map[string]interface{}{"type": "raw"}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if datasetId != "pid-123" {
		t.Fatalf("expected pid-123, got %s", datasetId)
	}
}

func TestRegisterDatasetRecordWrapsError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	svc := &IngestService{Base: testIngestServiceBase(server.URL, server.Client())}

	_, err := svc.RegisterDatasetRecord(map[string]interface{}{"type": "raw"}, nil)
	if err == nil {
		t.Fatalf("expected error from failed dataset ingest")
	}
	if !strings.Contains(err.Error(), "failed to ingest dataset record") {
		t.Fatalf("expected wrapped error message, got: %v", err)
	}
}

func TestIngestAppendsArchivableWhenCopyDisabled(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/datasets" {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"pid":"pid-no-copy"}`))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	base := testIngestServiceBase(server.URL, server.Client())
	svc := &IngestService{Base: base, FileSys: &FileService{}}

	batch := &DatasetBatch{MetaDataMap: map[string]interface{}{
		"type":           "raw",
		"classification": "IN=medium,AV=low,CO=low",
	}}

	datasetId, err := svc.Ingest(
		batch,
		"/data/run1",
		nil,
		time.Now().Add(-time.Hour),
		time.Now(),
		"owner-group",
		false,
		DatasetIngestRuntimeConfig{Tapecopies: 1},
	)
	if err != nil {
		t.Fatalf("unexpected ingest error: %v", err)
	}
	if datasetId != "pid-no-copy" {
		t.Fatalf("unexpected dataset id: %s", datasetId)
	}
	if len(svc.ArchivableDatasetIDs) != 1 || svc.ArchivableDatasetIDs[0] != "pid-no-copy" {
		t.Fatalf("expected dataset to be marked archivable, got %#v", svc.ArchivableDatasetIDs)
	}

	lifecycle := batch.MetaDataMap["datasetlifecycle"].(map[string]interface{})
	if lifecycle["archivable"] != true || lifecycle["archiveStatusMessage"] != "datasetCreated" {
		t.Fatalf("unexpected lifecycle state: %#v", lifecycle)
	}
	if batch.MetaDataMap["sourceFolder"] != "/data/run1" {
		t.Fatalf("expected sourceFolder to be updated")
	}
}

func TestIngestSkipsArchivableWhenCopyEnabledAndTransferNotArchivable(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/datasets" {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"pid":"pid-copy"}`))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	base := testIngestServiceBase(server.URL, server.Client())
	fileSys := &FileService{}
	fileSys.TransferFiles = func(params cliutils.TransferParams) (bool, error) {
		return false, nil
	}

	svc := &IngestService{Base: base, FileSys: fileSys}
	batch := &DatasetBatch{MetaDataMap: map[string]interface{}{
		"type":           "raw",
		"classification": "IN=medium,AV=low,CO=low",
	}}

	_, err := svc.Ingest(
		batch,
		"/data/run2",
		nil,
		time.Now().Add(-2*time.Hour),
		time.Now(),
		"owner-group",
		true,
		DatasetIngestRuntimeConfig{RSYNCServer: "rsync.psi.ch"},
	)
	if err != nil {
		t.Fatalf("unexpected ingest error: %v", err)
	}
	if len(svc.ArchivableDatasetIDs) != 0 {
		t.Fatalf("expected no archivable ids when transfer returns non-archivable, got %#v", svc.ArchivableDatasetIDs)
	}

	lifecycle := batch.MetaDataMap["datasetlifecycle"].(map[string]interface{})
	if lifecycle["archivable"] != false || lifecycle["archiveStatusMessage"] != "filesNotYetAvailable" {
		t.Fatalf("unexpected lifecycle state: %#v", lifecycle)
	}
}
