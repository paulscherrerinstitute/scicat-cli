package datasetIngestor

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestBuildDestPaths(t *testing.T) {
	tests := []struct {
		name           string
		datasetId      string
		RSYNCServer    string
		sourceFolder   string
		wantShareRoot  string
		wantDestFolder string
	}{
		{
			name:           "basic unix-style source, no port",
			datasetId:      "prefix/abc123",
			RSYNCServer:    "fileserver.example.com",
			sourceFolder:   `/data/experiment1`,
			wantShareRoot:  `\\fileserver.example.com\archive`,
			wantDestFolder: `\\fileserver.example.com\archive\abc123\data\experiment1`,
		},
		{
			name:           "server with port number",
			datasetId:      "prefix/def456",
			RSYNCServer:    "fileserver.example.com:22",
			sourceFolder:   `/data/experiment2`,
			wantShareRoot:  `\\fileserver.example.com\archive`,
			wantDestFolder: `\\fileserver.example.com\archive\def456\data\experiment2`,
		},
		{
			name:           "source folder with drive letter",
			datasetId:      "prefix/ghi789",
			RSYNCServer:    "fileserver.example.com",
			sourceFolder:   `C:\Users\scicat\data`,
			wantShareRoot:  `\\fileserver.example.com\archive`,
			wantDestFolder: `\\fileserver.example.com\archive\ghi789\Users\scicat\data`,
		},
		{
			name:           "source folder with drive letter and port",
			datasetId:      "prefix/jkl012",
			RSYNCServer:    "10.0.0.1:2222",
			sourceFolder:   `D:\experiments\run42`,
			wantShareRoot:  `\\10.0.0.1\archive`,
			wantDestFolder: `\\10.0.0.1\archive\jkl012\experiments\run42`,
		},
		{
			name:           "source folder unix-style with drive prefix",
			datasetId:      "prefix/mno345",
			RSYNCServer:    "server",
			sourceFolder:   `C:/data/folder`,
			wantShareRoot:  `\\server\archive`,
			wantDestFolder: `\\server\archive\mno345\data\folder`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			shareRoot, destFolder := buildDestPaths(tt.datasetId, tt.RSYNCServer, tt.sourceFolder)
			if shareRoot != tt.wantShareRoot {
				t.Errorf("shareRoot = %q, want %q", shareRoot, tt.wantShareRoot)
			}
			if destFolder != tt.wantDestFolder {
				t.Errorf("destFolder = %q, want %q", destFolder, tt.wantDestFolder)
			}
		})
	}
}

func TestBuildNetUseArgs(t *testing.T) {
	tests := []struct {
		name     string
		share    string
		username string
		password string
		wantArgs string
	}{
		{
			name:     "with password (NTLM fallback)",
			share:    `\\server\archive`,
			username: "jdoe",
			password: "secret",
			wantArgs: `use \\server\archive secret /user:jdoe`,
		},
		{
			name:     "empty password (Kerberos)",
			share:    `\\server\archive`,
			username: "jdoe",
			password: "",
			wantArgs: `use \\server\archive /user:jdoe`,
		},
		{
			name:     "domain-qualified username",
			share:    `\\fileserver.example.com\archive`,
			username: `DOMAIN\jdoe`,
			password: "pw",
			wantArgs: `use \\fileserver.example.com\archive pw /user:DOMAIN\jdoe`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			args := buildNetUseArgs(tt.share, tt.username, tt.password)
			got := strings.Join(args, " ")
			if got != tt.wantArgs {
				t.Errorf("buildNetUseArgs() = %q, want %q", got, tt.wantArgs)
			}
		})
	}
}

func TestRunRobocopyFullDir(t *testing.T) {
	srcDir := t.TempDir()
	destDir := t.TempDir()

	// Create test files in source
	files := map[string]string{
		"file1.txt":          "hello",
		"subdir/file2.txt":   "world",
		"subdir/a/file3.dat": "nested",
	}
	for relPath, content := range files {
		abs := filepath.Join(srcDir, relPath)
		if err := os.MkdirAll(filepath.Dir(abs), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(abs, []byte(content), 0644); err != nil {
			t.Fatal(err)
		}
	}

	var output bytes.Buffer
	err := runRobocopy(&output, srcDir, destDir, "/E")
	if err != nil {
		t.Fatalf("runRobocopy() error: %v\nOutput:\n%s", err, output.String())
	}

	// Verify all files were copied
	for relPath, wantContent := range files {
		abs := filepath.Join(destDir, relPath)
		got, err := os.ReadFile(abs)
		if err != nil {
			t.Errorf("expected file %s not found: %v", relPath, err)
			continue
		}
		if string(got) != wantContent {
			t.Errorf("file %s: got %q, want %q", relPath, string(got), wantContent)
		}
	}
}

func TestRunRobocopySingleFile(t *testing.T) {
	srcDir := t.TempDir()
	destDir := t.TempDir()

	if err := os.WriteFile(filepath.Join(srcDir, "target.txt"), []byte("data"), 0644); err != nil {
		t.Fatal(err)
	}
	// Also create a file that should NOT be copied
	if err := os.WriteFile(filepath.Join(srcDir, "other.txt"), []byte("ignored"), 0644); err != nil {
		t.Fatal(err)
	}

	var output bytes.Buffer
	err := runRobocopy(&output, srcDir, destDir, "target.txt")
	if err != nil {
		t.Fatalf("runRobocopy() error: %v\nOutput:\n%s", err, output.String())
	}

	// target.txt should exist
	got, err := os.ReadFile(filepath.Join(destDir, "target.txt"))
	if err != nil {
		t.Fatalf("target.txt not copied: %v", err)
	}
	if string(got) != "data" {
		t.Errorf("target.txt content = %q, want %q", string(got), "data")
	}

	// other.txt should NOT exist
	if _, err := os.Stat(filepath.Join(destDir, "other.txt")); !os.IsNotExist(err) {
		t.Error("other.txt should not have been copied")
	}
}
