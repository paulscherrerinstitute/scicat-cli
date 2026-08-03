package orchestrator

import (
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/paulscherrerinstitute/scicat-cli/v3/datasetIngestor"
	"github.com/paulscherrerinstitute/scicat-cli/v3/datasetUtils"
)

// withCompleteIngestMocks installs default (successful, no-op) mocks for all of CompleteIngest's
// dependencies and restores the originals on test cleanup. Each test then only needs to override
// whichever dependency it actually cares about.
func withCompleteIngestMocks(t *testing.T) {
	t.Helper()
	oldGetDatasetDetails := getDatasetDetailsFunc
	oldGather := gatherCompletionFileListFunc
	oldCreateOrigDatablocks := createOrigDatablocksFunc
	oldPatchDataset := patchDatasetFunc
	oldMarkFilesReady := markFilesReadyFunc
	t.Cleanup(func() {
		getDatasetDetailsFunc = oldGetDatasetDetails
		gatherCompletionFileListFunc = oldGather
		createOrigDatablocksFunc = oldCreateOrigDatablocks
		patchDatasetFunc = oldPatchDataset
		markFilesReadyFunc = oldMarkFilesReady
	})

	getDatasetDetailsFunc = func(client *http.Client, APIServer string, accessToken string, datasetList []string, ownerGroup string) ([]datasetUtils.Dataset, []string, error) {
		return []datasetUtils.Dataset{{Pid: "testPid", SourceFolder: "/some/folder", NumberOfFiles: 0}}, nil, nil
	}
	gatherCompletionFileListFunc = func(sourceFolder string) ([]datasetIngestor.Datafile, time.Time, time.Time, uint, uint, error) {
		return []datasetIngestor.Datafile{{Path: "a"}}, time.Now(), time.Now(), 0, 0, nil
	}
	createOrigDatablocksFunc = func(client *http.Client, APIServer string, fullFileArray []datasetIngestor.Datafile, datasetId string, user map[string]string) error {
		return nil
	}
	patchDatasetFunc = func(client *http.Client, APIServer string, token string, datasetId string, meta map[string]interface{}) error {
		return nil
	}
	markFilesReadyFunc = func(client *http.Client, APIServer string, datasetId string, user map[string]string) error {
		return nil
	}
}

func TestCompleteIngest(t *testing.T) {
	archiveManager := map[string]string{"username": "archiveManager", "accessToken": "testToken"}

	t.Run("rejects non archiveManager users", func(t *testing.T) {
		err := CompleteIngest(nil, "", map[string]string{"username": "someoneElse"}, "testPid")
		if err == nil {
			t.Fatal("expected an error, got nil")
		}
	})

	resolutionFailures := []struct {
		name                  string
		mockGetDatasetDetails func(client *http.Client, APIServer string, accessToken string, datasetList []string, ownerGroup string) ([]datasetUtils.Dataset, []string, error)
		mockGather            func(sourceFolder string) ([]datasetIngestor.Datafile, time.Time, time.Time, uint, uint, error)
		checkErr              func(t *testing.T, err error)
	}{
		{
			name: "the dataset already contains files",
			mockGetDatasetDetails: func(client *http.Client, APIServer string, accessToken string, datasetList []string, ownerGroup string) ([]datasetUtils.Dataset, []string, error) {
				return []datasetUtils.Dataset{{Pid: "testPid", SourceFolder: "/some/folder", NumberOfFiles: 3}}, nil, nil
			},
		},
		{
			name: "the dataset has no sourceFolder",
			mockGetDatasetDetails: func(client *http.Client, APIServer string, accessToken string, datasetList []string, ownerGroup string) ([]datasetUtils.Dataset, []string, error) {
				return []datasetUtils.Dataset{{Pid: "testPid", SourceFolder: "", NumberOfFiles: 0}}, nil, nil
			},
		},
		{
			name: "the dataset is not found",
			mockGetDatasetDetails: func(client *http.Client, APIServer string, accessToken string, datasetList []string, ownerGroup string) ([]datasetUtils.Dataset, []string, error) {
				return nil, []string{"testPid"}, nil
			},
		},
		{
			name: "the sourceFolder contains no files",
			mockGather: func(sourceFolder string) ([]datasetIngestor.Datafile, time.Time, time.Time, uint, uint, error) {
				return nil, time.Time{}, time.Time{}, 0, 0, &datasetIngestor.EmptyDatasetError{SourceFolder: sourceFolder}
			},
			checkErr: func(t *testing.T, err error) {
				var emptyDatasetErr *datasetIngestor.EmptyDatasetError
				if !errors.As(err, &emptyDatasetErr) {
					t.Fatalf("expected an *EmptyDatasetError, got: %v (%T)", err, err)
				}
			},
		},
	}
	for _, tt := range resolutionFailures {
		t.Run("fails when "+tt.name, func(t *testing.T) {
			withCompleteIngestMocks(t)
			if tt.mockGetDatasetDetails != nil {
				getDatasetDetailsFunc = tt.mockGetDatasetDetails
			}
			if tt.mockGather != nil {
				gatherCompletionFileListFunc = tt.mockGather
			}

			err := CompleteIngest(nil, "", archiveManager, "testPid")
			if tt.checkErr != nil {
				tt.checkErr(t, err)
			} else if err == nil {
				t.Fatal("expected an error, got nil")
			}
		})
	}

	warnings := []struct {
		name             string
		skippedLinks     uint
		illegalFileNames uint
		checkWarning     func(t *testing.T, err error)
	}{
		{
			name:         "SkippedLinksWarning when links were skipped",
			skippedLinks: 1,
			checkWarning: func(t *testing.T, err error) {
				var w *datasetIngestor.SkippedLinksWarning
				if !errors.As(err, &w) {
					t.Fatalf("expected a *SkippedLinksWarning, got: %v (%T)", err, err)
				}
				if w.Count != 1 {
					t.Errorf("expected 1 skipped link, got %d", w.Count)
				}
			},
		},
		{
			name:             "IllegalFileNamesWarning when a filename is illegal",
			illegalFileNames: 1,
			checkWarning: func(t *testing.T, err error) {
				var w *datasetIngestor.IllegalFileNamesWarning
				if !errors.As(err, &w) {
					t.Fatalf("expected an *IllegalFileNamesWarning, got: %v (%T)", err, err)
				}
				if w.Count != 1 {
					t.Errorf("expected 1 illegal file name, got %d", w.Count)
				}
			},
		},
	}
	for _, tt := range warnings {
		t.Run("creates the origdatablock and returns a "+tt.name, func(t *testing.T) {
			withCompleteIngestMocks(t)
			gatherCompletionFileListFunc = func(sourceFolder string) ([]datasetIngestor.Datafile, time.Time, time.Time, uint, uint, error) {
				return []datasetIngestor.Datafile{{Path: "a"}}, time.Now(), time.Now(), tt.skippedLinks, tt.illegalFileNames, nil
			}
			var createdOrigDatablock bool
			createOrigDatablocksFunc = func(client *http.Client, APIServer string, fullFileArray []datasetIngestor.Datafile, datasetId string, user map[string]string) error {
				createdOrigDatablock = true
				return nil
			}

			err := CompleteIngest(nil, "", archiveManager, "testPid")
			tt.checkWarning(t, err)
			if !createdOrigDatablock {
				t.Error("expected an origdatablock to be created even when a warning is returned")
			}
		})
	}

	t.Run("gathers the filelist and creates the origdatablocks", func(t *testing.T) {
		withCompleteIngestMocks(t)
		var createdOrigDatablock bool
		createOrigDatablocksFunc = func(client *http.Client, APIServer string, fullFileArray []datasetIngestor.Datafile, datasetId string, user map[string]string) error {
			createdOrigDatablock = true
			return nil
		}

		if err := CompleteIngest(nil, "", archiveManager, "testPid"); err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if !createdOrigDatablock {
			t.Error("expected an origdatablock to be created")
		}
	})

	t.Run("aborts before updating dataset times or marking files ready when creating origdatablocks fails", func(t *testing.T) {
		withCompleteIngestMocks(t)
		createOrigDatablocksFunc = func(client *http.Client, APIServer string, fullFileArray []datasetIngestor.Datafile, datasetId string, user map[string]string) error {
			return errors.New("boom")
		}
		var updatedTimes, markedFilesReady bool
		patchDatasetFunc = func(client *http.Client, APIServer string, token string, datasetId string, meta map[string]interface{}) error {
			updatedTimes = true
			return nil
		}
		markFilesReadyFunc = func(client *http.Client, APIServer string, datasetId string, user map[string]string) error {
			markedFilesReady = true
			return nil
		}

		err := CompleteIngest(nil, "", archiveManager, "testPid")
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
		withCompleteIngestMocks(t)
		patchDatasetFunc = func(client *http.Client, APIServer string, token string, datasetId string, meta map[string]interface{}) error {
			return errors.New("boom")
		}
		var markedFilesReady bool
		markFilesReadyFunc = func(client *http.Client, APIServer string, datasetId string, user map[string]string) error {
			markedFilesReady = true
			return nil
		}

		err := CompleteIngest(nil, "", archiveManager, "testPid")
		if err == nil {
			t.Fatal("expected an error, got nil")
		}
		if markedFilesReady {
			t.Error("expected files not to be marked ready when updating dataset times failed")
		}
	})

	t.Run("PATCHes the dataset's creationTime and endTime derived from the scanned files", func(t *testing.T) {
		withCompleteIngestMocks(t)
		wantStartTime := time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC)
		wantEndTime := time.Date(2020, 6, 7, 8, 9, 10, 0, time.UTC)
		gatherCompletionFileListFunc = func(sourceFolder string) ([]datasetIngestor.Datafile, time.Time, time.Time, uint, uint, error) {
			return []datasetIngestor.Datafile{{Path: "a"}}, wantStartTime, wantEndTime, 0, 0, nil
		}
		var patchedMeta map[string]interface{}
		patchDatasetFunc = func(client *http.Client, APIServer string, token string, datasetId string, meta map[string]interface{}) error {
			patchedMeta = meta
			return nil
		}

		if err := CompleteIngest(nil, "", archiveManager, "testPid"); err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		if got := patchedMeta["creationTime"]; got != wantStartTime.Format(time.RFC3339) {
			t.Errorf("creationTime = %v, want %v", got, wantStartTime.Format(time.RFC3339))
		}
		if got := patchedMeta["endTime"]; got != wantEndTime.Format(time.RFC3339) {
			t.Errorf("endTime = %v, want %v", got, wantEndTime.Format(time.RFC3339))
		}
	})
}

// --- gatherCompletionFileList ---
//
// Unlike CompleteIngest's other dependencies, gatherCompletionFileList does real local filesystem
// scanning (via datasetIngestor.GetValidatedLocalFileList), so it's tested directly against real
// files rather than through a mock.

func TestGatherCompletionFileList(t *testing.T) {
	t.Run("skips external symlinks and counts them", func(t *testing.T) {
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

		_, _, _, skippedLinks, illegalFileNames, err := gatherCompletionFileList(tempDirAbs)
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if skippedLinks != 1 {
			t.Errorf("expected 1 skipped link, got %d", skippedLinks)
		}
		if illegalFileNames != 0 {
			t.Errorf("expected 0 illegal file names, got %d", illegalFileNames)
		}
	})

	t.Run("excludes illegal filenames and counts them", func(t *testing.T) {
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

		_, _, _, skippedLinks, illegalFileNames, err := gatherCompletionFileList(tempDir)
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if illegalFileNames != 1 {
			t.Errorf("expected 1 illegal file name, got %d", illegalFileNames)
		}
		if skippedLinks != 0 {
			t.Errorf("expected 0 skipped links, got %d", skippedLinks)
		}
	})

	t.Run("propagates the underlying error for an empty sourceFolder", func(t *testing.T) {
		tempDir, err := os.MkdirTemp("./", "test")
		if err != nil {
			t.Fatalf("failed to create temp directory: %s", err)
		}
		defer os.RemoveAll(tempDir)

		_, _, _, _, _, err = gatherCompletionFileList(tempDir)
		var emptyDatasetErr *datasetIngestor.EmptyDatasetError
		if !errors.As(err, &emptyDatasetErr) {
			t.Fatalf("expected an *EmptyDatasetError, got: %v (%T)", err, err)
		}
	})
}

func TestExtractPidFromArgs(t *testing.T) {
	t.Run("returns the pid when a single valid arg is given", func(t *testing.T) {
		pid, err := ExtractPidFromArgs([]string{"20.500.11935/testPid"})
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if pid != "20.500.11935/testPid" {
			t.Errorf("expected pid %q, got %q", "20.500.11935/testPid", pid)
		}
	})

	t.Run("fails when no args are given", func(t *testing.T) {
		_, err := ExtractPidFromArgs([]string{})
		if err == nil {
			t.Fatal("expected an error, got nil")
		}
	})

	t.Run("fails when more than one arg is given", func(t *testing.T) {
		_, err := ExtractPidFromArgs([]string{"20.500.11935/testPid", "extra"})
		if err == nil {
			t.Fatal("expected an error, got nil")
		}
	})

	t.Run("fails when the pid does not have the expected prefix", func(t *testing.T) {
		_, err := ExtractPidFromArgs([]string{"someOtherPid"})
		if err == nil {
			t.Fatal("expected an error, got nil")
		}
	})
}
