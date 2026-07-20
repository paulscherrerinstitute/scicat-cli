package datasetIngestor

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCreateLocalSymlinkCallbackForFileLister(t *testing.T) {
	sourceDir, err := os.MkdirTemp("./", "source")
	if err != nil {
		t.Fatalf("failed to create source directory: %s", err)
	}
	defer os.RemoveAll(sourceDir)
	sourceDirAbs, err := filepath.Abs(sourceDir)
	if err != nil {
		t.Fatalf("failed to resolve source directory: %s", err)
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

	internalTarget := filepath.Join(sourceDirAbs, "internal.txt")
	if err := os.WriteFile(internalTarget, []byte("test"), 0644); err != nil {
		t.Fatalf("failed to create internal target file: %s", err)
	}
	internalLink := filepath.Join(sourceDirAbs, "internalLink.txt")
	if err := os.Symlink(internalTarget, internalLink); err != nil {
		t.Fatalf("failed to create internal symlink: %s", err)
	}

	externalTarget := filepath.Join(outsideDirAbs, "external.txt")
	if err := os.WriteFile(externalTarget, []byte("test"), 0644); err != nil {
		t.Fatalf("failed to create external target file: %s", err)
	}
	externalLink := filepath.Join(sourceDirAbs, "externalLink.txt")
	if err := os.Symlink(externalTarget, externalLink); err != nil {
		t.Fatalf("failed to create external symlink: %s", err)
	}

	testCases := []struct {
		name         string
		policy       string
		symlinkPath  string
		expectedKeep bool
	}{
		{"kA always keeps internal links", "kA", internalLink, true},
		{"kA always keeps external links", "kA", externalLink, true},
		{"sA always skips internal links", "sA", internalLink, false},
		{"sA always skips external links", "sA", externalLink, false},
		{"dA keeps internal links", "dA", internalLink, true},
		{"dA skips external links", "dA", externalLink, false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			policy := tc.policy
			var skippedLinks uint
			callback := CreateLocalSymlinkCallbackForFileLister(&policy, &skippedLinks)

			keep, err := callback(tc.symlinkPath, sourceDirAbs)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if keep != tc.expectedKeep {
				t.Errorf("expected keep=%v, got %v", tc.expectedKeep, keep)
			}
			if !keep && skippedLinks != 1 {
				t.Errorf("expected skippedLinks to be incremented, got %d", skippedLinks)
			}
			if keep && skippedLinks != 0 {
				t.Errorf("expected skippedLinks to stay at 0, got %d", skippedLinks)
			}
		})
	}
}

func TestCreateLocalFilenameFilterCallback(t *testing.T) {
	testCases := []struct {
		name         string
		filepath     string
		expectedKeep bool
	}{
		{"plain filename is kept", "some/normal/file.txt", true},
		{"asterisk is rejected", "some/file*.txt", false},
		{"backslash is rejected", `some\file.txt`, false},
		{"triple blank is rejected", "some/file   name.txt", false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var illegalFileNames uint
			callback := CreateLocalFilenameFilterCallback(&illegalFileNames)

			keep := callback(tc.filepath)
			if keep != tc.expectedKeep {
				t.Errorf("expected keep=%v, got %v", tc.expectedKeep, keep)
			}
			if !keep && illegalFileNames != 1 {
				t.Errorf("expected illegalFileNames to be incremented, got %d", illegalFileNames)
			}
			if keep && illegalFileNames != 0 {
				t.Errorf("expected illegalFileNames to stay at 0, got %d", illegalFileNames)
			}
		})
	}
}
