package datasetIngestor

import (
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
	skip := ""
	fullFileArray, startTime, endTime, _, numFiles, totalSize, err := GetLocalFileList(tempDir, "", &skip)
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
