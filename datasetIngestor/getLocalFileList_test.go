package datasetIngestor

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestAssembleFilelisting(t *testing.T) {
	// Create a temporary directory
	tempDir, err := os.MkdirTemp("./", "test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %s", err)
	}
	defer os.RemoveAll(tempDir)

	// Create a file in the temporary directory
	fileName := "testfile"
	filePath := filepath.Join(tempDir, fileName)
	if err := os.WriteFile(filePath, []byte("test"), 0644); err != nil {
		t.Fatalf("Failed to create test file: %s", err)
	}

	// Call AssembleFilelisting on the temporary directory
	fullFileArray, startTime, endTime, _, numFiles, totalSize, err := GetLocalFileList(tempDir, "", nil, nil)
	if err != nil {
		t.Errorf("got error: %v", err)
	}

	// Check that the returned file array contains the correct file
	if len(fullFileArray) != 1 {
		t.Fatalf("Expected 1 file, got %d", len(fullFileArray))
	}
	if fullFileArray[0].Path != fileName {
		t.Errorf("Expected file path %s, got %s", fileName, fullFileArray[0].Path)
	}
	if fullFileArray[0].Size != 4 {
		t.Errorf("Expected file size 4, got %d", fullFileArray[0].Size)
	}
	fileTime, err := time.Parse(time.RFC3339, fullFileArray[0].Time)
	if err != nil {
		t.Fatalf("Failed to parse file time: %s", err)
	}
	if time.Since(fileTime) > time.Second {
		t.Errorf("Expected file time within 1 second of now")
	}

	// Check the other outputs of AssembleFilelisting
	if time.Since(startTime) > time.Second {
		t.Errorf("Expected start time within 1 second of now - res: %s", startTime)
	}
	if time.Since(endTime) > time.Second {
		t.Errorf("Expected end time within 1 second of now")
	}
	if numFiles != 1 {
		t.Errorf("Expected numFiles to be 1, got %d", numFiles)
	}
	if totalSize != 4 {
		t.Errorf("Expected totalSize to be 4, got %d", totalSize)
	}
}

func TestEmptyDatasetErrorMessage(t *testing.T) {
	err := &EmptyDatasetError{SourceFolder: "/some/folder"}
	want := `"/some/folder" dataset cannot be ingested - contains no files`
	if got := err.Error(); got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestTooManyFilesErrorMessage(t *testing.T) {
	err := &TooManyFilesError{SourceFolder: "/some/folder", NumFiles: 5, MaxFiles: 3}
	want := `"/some/folder" dataset cannot be ingested - too many files: has 5, max. 3`
	if got := err.Error(); got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestSkippedLinksWarningMessage(t *testing.T) {
	warning := &SkippedLinksWarning{Count: 2}
	want := "Total number of link files skipped:2"
	if got := warning.Error(); got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestIllegalFileNamesWarningMessage(t *testing.T) {
	warning := &IllegalFileNamesWarning{Count: 3}
	want := "Total number of illegal file names skipped:3"
	if got := warning.Error(); got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestGetValidatedLocalFileList(t *testing.T) {
	t.Run("returns the file list when the dataset is not empty", func(t *testing.T) {
		tempDir, err := os.MkdirTemp("./", "test")
		if err != nil {
			t.Fatalf("Failed to create temp directory: %s", err)
		}
		defer os.RemoveAll(tempDir)

		filePath := filepath.Join(tempDir, "testfile")
		if err := os.WriteFile(filePath, []byte("test"), 0644); err != nil {
			t.Fatalf("Failed to create test file: %s", err)
		}

		fullFileArray, _, _, _, numFiles, totalSize, err := GetValidatedLocalFileList(tempDir, "", nil, nil)
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if len(fullFileArray) != 1 || numFiles != 1 {
			t.Errorf("expected 1 file, got %d entries and numFiles=%d", len(fullFileArray), numFiles)
		}
		if totalSize != 4 {
			t.Errorf("expected totalSize 4, got %d", totalSize)
		}
	})

	t.Run("returns an EmptyDatasetError when the sourceFolder contains no files", func(t *testing.T) {
		tempDir, err := os.MkdirTemp("./", "test")
		if err != nil {
			t.Fatalf("Failed to create temp directory: %s", err)
		}
		defer os.RemoveAll(tempDir)

		_, _, _, _, _, _, err = GetValidatedLocalFileList(tempDir, "", nil, nil)
		var emptyDatasetErr *EmptyDatasetError
		if !errors.As(err, &emptyDatasetErr) {
			t.Fatalf("expected an *EmptyDatasetError, got: %v (%T)", err, err)
		}
	})

	t.Run("wraps the underlying error when the sourceFolder does not exist", func(t *testing.T) {
		_, _, _, _, _, _, err := GetValidatedLocalFileList("./does-not-exist", "", nil, nil)
		if err == nil {
			t.Fatal("expected an error, got nil")
		}
		var emptyDatasetErr *EmptyDatasetError
		if errors.As(err, &emptyDatasetErr) {
			t.Fatalf("expected a plain gathering error, not an *EmptyDatasetError: %v", err)
		}
	})
}
