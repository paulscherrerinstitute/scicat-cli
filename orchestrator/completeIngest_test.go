package orchestrator

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/paulscherrerinstitute/scicat-cli/v3/datasetIngestor"
)

func newDatasetDetailsServer(t *testing.T, sourceFolder string, numberOfFiles int) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		switch {
		case req.Method == http.MethodGet && req.URL.Path == "/Datasets":
			rw.WriteHeader(http.StatusOK)
			fmt.Fprintf(rw, `[{"pid":"testPid","sourceFolder":%q,"numberOfFiles":%d}]`, sourceFolder, numberOfFiles)
		case req.Method == http.MethodPatch && req.URL.Path == "/Datasets/testPid":
			rw.WriteHeader(http.StatusOK)
		case req.Method == http.MethodPatch && req.URL.Path == "/Datasets/testPid/datasetlifecycle":
			rw.WriteHeader(http.StatusOK)
		case req.Method == http.MethodPost && req.URL.Path == "/origdatablocks":
			rw.WriteHeader(http.StatusOK)
		default:
			t.Errorf("unexpected request: %s %s", req.Method, req.URL.Path)
			rw.WriteHeader(http.StatusNotFound)
		}
	}))
}

func TestCompleteIngest(t *testing.T) {
	archiveManager := map[string]string{"username": "archiveManager", "accessToken": "testToken"}

	t.Run("rejects non archiveManager users", func(t *testing.T) {
		err := CompleteIngest(http.DefaultClient, "", map[string]string{"username": "someoneElse"}, "testPid")
		if err == nil {
			t.Fatal("expected an error, got nil")
		}
	})

	t.Run("fails when the dataset already contains files", func(t *testing.T) {
		server := newDatasetDetailsServer(t, "/some/folder", 3)
		defer server.Close()

		err := CompleteIngest(server.Client(), server.URL, archiveManager, "testPid")
		if err == nil {
			t.Fatal("expected an error, got nil")
		}
	})

	t.Run("fails when the dataset has no sourceFolder", func(t *testing.T) {
		server := newDatasetDetailsServer(t, "", 0)
		defer server.Close()

		err := CompleteIngest(server.Client(), server.URL, archiveManager, "testPid")
		if err == nil {
			t.Fatal("expected an error, got nil")
		}
	})

	t.Run("fails when the dataset is not found", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			rw.WriteHeader(http.StatusOK)
			fmt.Fprint(rw, `[]`)
		}))
		defer server.Close()

		err := CompleteIngest(server.Client(), server.URL, archiveManager, "testPid")
		if err == nil {
			t.Fatal("expected an error, got nil")
		}
	})

	t.Run("fails when the sourceFolder contains no files", func(t *testing.T) {
		tempDir, err := os.MkdirTemp("./", "test")
		if err != nil {
			t.Fatalf("failed to create temp directory: %s", err)
		}
		defer os.RemoveAll(tempDir)

		server := newDatasetDetailsServer(t, tempDir, 0)
		defer server.Close()

		err = CompleteIngest(server.Client(), server.URL, archiveManager, "testPid")
		var emptyDatasetErr *datasetIngestor.EmptyDatasetError
		if !errors.As(err, &emptyDatasetErr) {
			t.Fatalf("expected an *EmptyDatasetError, got: %v (%T)", err, err)
		}
	})

	t.Run("creates the origdatablock and returns a SkippedLinksWarning when links were skipped", func(t *testing.T) {
		tempDir, err := os.MkdirTemp("./", "test")
		if err != nil {
			t.Fatalf("failed to create temp directory: %s", err)
		}
		defer os.RemoveAll(tempDir)
		tempDirAbs, err := filepath.Abs(tempDir)
		if err != nil {
			t.Fatalf("failed to resolve temp directory: %s", err)
		}

		outsideDir, err := os.MkdirTemp("./", "outside")
		if err != nil {
			t.Fatalf("failed to create outside directory: %s", err)
		}
		defer os.RemoveAll(outsideDir)
		outsideDirAbs, err := filepath.Abs(outsideDir)
		if err != nil {
			t.Fatalf("failed to resolve outside directory: %s", err)
		}

		externalTarget := filepath.Join(outsideDirAbs, "external.txt")
		if err := os.WriteFile(externalTarget, []byte("test"), 0644); err != nil {
			t.Fatalf("failed to create external target file: %s", err)
		}
		externalLink := filepath.Join(tempDirAbs, "externalLink.txt")
		if err := os.Symlink(externalTarget, externalLink); err != nil {
			t.Fatalf("failed to create external symlink: %s", err)
		}
		// a plain file must remain so the dataset isn't also empty
		if err := os.WriteFile(filepath.Join(tempDirAbs, "regular.txt"), []byte("test"), 0644); err != nil {
			t.Fatalf("failed to create regular file: %s", err)
		}

		var createdOrigDatablock bool
		server := newDatasetDetailsServerWithOrigDatablockTracking(t, tempDirAbs, &createdOrigDatablock)
		defer server.Close()

		err = CompleteIngest(server.Client(), server.URL, archiveManager, "testPid")
		var skippedLinksWarning *datasetIngestor.SkippedLinksWarning
		if !errors.As(err, &skippedLinksWarning) {
			t.Fatalf("expected a *SkippedLinksWarning, got: %v (%T)", err, err)
		}
		if skippedLinksWarning.Count != 1 {
			t.Errorf("expected 1 skipped link, got %d", skippedLinksWarning.Count)
		}
		if !createdOrigDatablock {
			t.Error("expected an origdatablock to be created even when links were skipped")
		}
	})

	t.Run("creates the origdatablock and returns an IllegalFileNamesWarning when a filename is illegal", func(t *testing.T) {
		tempDir, err := os.MkdirTemp("./", "test")
		if err != nil {
			t.Fatalf("failed to create temp directory: %s", err)
		}
		defer os.RemoveAll(tempDir)

		// three consecutive blanks are illegal per CreateLocalFilenameFilterCallback; unlike "*" or
		// "\", this is a valid filename on Windows too, so the file can actually be created here.
		illegalFilePath := filepath.Join(tempDir, "illegal   file.txt")
		if err := os.WriteFile(illegalFilePath, []byte("test"), 0644); err != nil {
			t.Fatalf("failed to create illegally named file: %s", err)
		}
		// a plain file must remain so the dataset isn't also empty
		if err := os.WriteFile(filepath.Join(tempDir, "regular.txt"), []byte("test"), 0644); err != nil {
			t.Fatalf("failed to create regular file: %s", err)
		}

		var createdOrigDatablock bool
		server := newDatasetDetailsServerWithOrigDatablockTracking(t, tempDir, &createdOrigDatablock)
		defer server.Close()

		err = CompleteIngest(server.Client(), server.URL, archiveManager, "testPid")
		var illegalFileNamesWarning *datasetIngestor.IllegalFileNamesWarning
		if !errors.As(err, &illegalFileNamesWarning) {
			t.Fatalf("expected an *IllegalFileNamesWarning, got: %v (%T)", err, err)
		}
		if illegalFileNamesWarning.Count != 1 {
			t.Errorf("expected 1 illegal file name, got %d", illegalFileNamesWarning.Count)
		}
		if !createdOrigDatablock {
			t.Error("expected an origdatablock to be created even when an illegal file name was found")
		}
	})

	t.Run("gathers the filelist and creates the origdatablocks", func(t *testing.T) {
		tempDir, err := os.MkdirTemp("./", "test")
		if err != nil {
			t.Fatalf("failed to create temp directory: %s", err)
		}
		defer os.RemoveAll(tempDir)

		filePath := filepath.Join(tempDir, "testfile")
		if err := os.WriteFile(filePath, []byte("test"), 0644); err != nil {
			t.Fatalf("failed to create test file: %s", err)
		}

		var createdOrigDatablock bool
		server := newDatasetDetailsServerWithOrigDatablockTracking(t, tempDir, &createdOrigDatablock)
		defer server.Close()

		if err := CompleteIngest(server.Client(), server.URL, archiveManager, "testPid"); err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if !createdOrigDatablock {
			t.Error("expected an origdatablock to be created")
		}
	})

	t.Run("aborts before updating dataset times or marking files ready when creating origdatablocks fails", func(t *testing.T) {
		tempDir, err := os.MkdirTemp("./", "test")
		if err != nil {
			t.Fatalf("failed to create temp directory: %s", err)
		}
		defer os.RemoveAll(tempDir)

		filePath := filepath.Join(tempDir, "testfile")
		if err := os.WriteFile(filePath, []byte("test"), 0644); err != nil {
			t.Fatalf("failed to create test file: %s", err)
		}

		var updatedTimes, markedFilesReady bool
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			switch {
			case req.Method == http.MethodGet && req.URL.Path == "/Datasets":
				rw.WriteHeader(http.StatusOK)
				fmt.Fprintf(rw, `[{"pid":"testPid","sourceFolder":%q,"numberOfFiles":0}]`, tempDir)
			case req.Method == http.MethodPatch && req.URL.Path == "/Datasets/testPid":
				updatedTimes = true
				rw.WriteHeader(http.StatusOK)
			case req.Method == http.MethodPatch && req.URL.Path == "/Datasets/testPid/datasetlifecycle":
				markedFilesReady = true
				rw.WriteHeader(http.StatusOK)
			case req.Method == http.MethodPost && req.URL.Path == "/origdatablocks":
				rw.WriteHeader(http.StatusInternalServerError)
			default:
				t.Errorf("unexpected request: %s %s", req.Method, req.URL.Path)
				rw.WriteHeader(http.StatusNotFound)
			}
		}))
		defer server.Close()

		err = CompleteIngest(server.Client(), server.URL, archiveManager, "testPid")
		if err == nil {
			t.Fatal("expected an error, got nil")
		}
		if updatedTimes {
			t.Error("expected dataset times not to be updated when creating origdatablocks failed")
		}
		if markedFilesReady {
			t.Error("expected files not to be marked ready when creating origdatablocks failed")
		}
	})

	t.Run("aborts before marking files ready when updating the dataset times fails", func(t *testing.T) {
		tempDir, err := os.MkdirTemp("./", "test")
		if err != nil {
			t.Fatalf("failed to create temp directory: %s", err)
		}
		defer os.RemoveAll(tempDir)

		filePath := filepath.Join(tempDir, "testfile")
		if err := os.WriteFile(filePath, []byte("test"), 0644); err != nil {
			t.Fatalf("failed to create test file: %s", err)
		}

		var markedFilesReady bool
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			switch {
			case req.Method == http.MethodGet && req.URL.Path == "/Datasets":
				rw.WriteHeader(http.StatusOK)
				fmt.Fprintf(rw, `[{"pid":"testPid","sourceFolder":%q,"numberOfFiles":0}]`, tempDir)
			case req.Method == http.MethodPatch && req.URL.Path == "/Datasets/testPid":
				rw.WriteHeader(http.StatusInternalServerError)
			case req.Method == http.MethodPatch && req.URL.Path == "/Datasets/testPid/datasetlifecycle":
				markedFilesReady = true
				rw.WriteHeader(http.StatusOK)
			case req.Method == http.MethodPost && req.URL.Path == "/origdatablocks":
				rw.WriteHeader(http.StatusOK)
			default:
				t.Errorf("unexpected request: %s %s", req.Method, req.URL.Path)
				rw.WriteHeader(http.StatusNotFound)
			}
		}))
		defer server.Close()

		err = CompleteIngest(server.Client(), server.URL, archiveManager, "testPid")
		if err == nil {
			t.Fatal("expected an error, got nil")
		}
		if markedFilesReady {
			t.Error("expected files not to be marked ready when updating dataset times failed")
		}
	})

	t.Run("PATCHes the dataset's creationTime and endTime derived from the scanned files", func(t *testing.T) {
		tempDir, err := os.MkdirTemp("./", "test")
		if err != nil {
			t.Fatalf("failed to create temp directory: %s", err)
		}
		defer os.RemoveAll(tempDir)

		filePath := filepath.Join(tempDir, "testfile")
		if err := os.WriteFile(filePath, []byte("test"), 0644); err != nil {
			t.Fatalf("failed to create test file: %s", err)
		}

		var patchedTimesBody map[string]interface{}
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			switch {
			case req.Method == http.MethodGet && req.URL.Path == "/Datasets":
				rw.WriteHeader(http.StatusOK)
				fmt.Fprintf(rw, `[{"pid":"testPid","sourceFolder":%q,"numberOfFiles":0}]`, tempDir)
			case req.Method == http.MethodPatch && req.URL.Path == "/Datasets/testPid":
				body, _ := io.ReadAll(req.Body)
				if err := json.Unmarshal(body, &patchedTimesBody); err != nil {
					t.Errorf("failed to unmarshal PATCH body: %v", err)
				}
				rw.WriteHeader(http.StatusOK)
			case req.Method == http.MethodPatch && req.URL.Path == "/Datasets/testPid/datasetlifecycle":
				rw.WriteHeader(http.StatusOK)
			case req.Method == http.MethodPost && req.URL.Path == "/origdatablocks":
				rw.WriteHeader(http.StatusOK)
			default:
				t.Errorf("unexpected request: %s %s", req.Method, req.URL.Path)
				rw.WriteHeader(http.StatusNotFound)
			}
		}))
		defer server.Close()

		if err := CompleteIngest(server.Client(), server.URL, archiveManager, "testPid"); err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		creationTime, ok := patchedTimesBody["creationTime"].(string)
		if !ok || creationTime == "" {
			t.Fatalf("expected a non-empty creationTime in the PATCH body, got: %v", patchedTimesBody)
		}
		if _, err := time.Parse(time.RFC3339, creationTime); err != nil {
			t.Errorf("expected creationTime to be RFC3339, got %q: %v", creationTime, err)
		}
		endTime, ok := patchedTimesBody["endTime"].(string)
		if !ok || endTime == "" {
			t.Fatalf("expected a non-empty endTime in the PATCH body, got: %v", patchedTimesBody)
		}
		if _, err := time.Parse(time.RFC3339, endTime); err != nil {
			t.Errorf("expected endTime to be RFC3339, got %q: %v", endTime, err)
		}
	})
}

func newDatasetDetailsServerWithOrigDatablockTracking(t *testing.T, sourceFolder string, createdOrigDatablock *bool) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		switch {
		case req.Method == http.MethodGet && req.URL.Path == "/Datasets":
			rw.WriteHeader(http.StatusOK)
			fmt.Fprintf(rw, `[{"pid":"testPid","sourceFolder":%q,"numberOfFiles":0}]`, sourceFolder)
		case req.Method == http.MethodPatch && req.URL.Path == "/Datasets/testPid":
			rw.WriteHeader(http.StatusOK)
		case req.Method == http.MethodPatch && req.URL.Path == "/Datasets/testPid/datasetlifecycle":
			rw.WriteHeader(http.StatusOK)
		case req.Method == http.MethodPost && req.URL.Path == "/origdatablocks":
			*createdOrigDatablock = true
			rw.WriteHeader(http.StatusOK)
		default:
			t.Errorf("unexpected request: %s %s", req.Method, req.URL.Path)
			rw.WriteHeader(http.StatusNotFound)
		}
	}))
}
