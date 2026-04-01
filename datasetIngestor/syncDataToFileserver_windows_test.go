package datasetIngestor

import (
	"regexp"
	"strings"
	"testing"
)

func TestWindowsDestFolderConstruction(t *testing.T) {
	tests := []struct {
		name           string
		datasetId      string
		sourceFolder   string
		wantDestFolder string
		wantDest2      string
	}{
		{
			name:           "backslash path with drive letter",
			datasetId:      "prefix/abc123",
			sourceFolder:   `C:\data\experiment\run1`,
			wantDestFolder: "archive/abc123/data/experiment",
			wantDest2:      "archive/abc123/data/experiment/run1",
		},
		{
			name:           "forward slash path without drive letter",
			datasetId:      "prefix/def456",
			sourceFolder:   "/data/experiment/run2",
			wantDestFolder: "archive/def456/data/experiment",
			wantDest2:      "archive/def456/data/experiment/run2",
		},
		{
			name:           "forward slash path with drive letter",
			datasetId:      "prefix/ghi789",
			sourceFolder:   "D:/projects/sample",
			wantDestFolder: "archive/ghi789/projects",
			wantDest2:      "archive/ghi789/projects/sample",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			shortDatasetId := strings.Split(tt.datasetId, "/")[1]
			ss := strings.Split(tt.sourceFolder, ":")
			destFull := ss[len(ss)-1]
			separator := "/"
			if strings.Index(destFull, "/") < 0 {
				separator = "\\"
			}
			destparts := strings.Split(destFull, separator)

			destFolder := "archive/" + shortDatasetId + strings.Join(destparts[0:len(destparts)-1], "/")
			destFolder2 := "archive/" + shortDatasetId + strings.Join(destparts[0:len(destparts)], "/")

			if destFolder != tt.wantDestFolder {
				t.Errorf("destFolder = %q, want %q", destFolder, tt.wantDestFolder)
			}
			if destFolder2 != tt.wantDest2 {
				t.Errorf("destFolder2 = %q, want %q", destFolder2, tt.wantDest2)
			}
		})
	}
}

func TestWindowsDriveLetterRegexp(t *testing.T) {
	re := regexp.MustCompile(`^\/([A-Z])\/`)

	tests := []struct {
		input string
		want  string
	}{
		{"/C/data/file.txt", "C:/data/file.txt"},
		{"/D/projects/sample", "D:/projects/sample"},
		{"/data/file.txt", "/data/file.txt"},
		{"C:/data/file.txt", "C:/data/file.txt"},
		{"/c/data/file.txt", "/c/data/file.txt"},
	}

	for _, tt := range tests {
		got := re.ReplaceAllString(tt.input, "$1:/")
		if got != tt.want {
			t.Errorf("regexp on %q = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestServerPortAppend(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"myserver", "myserver:22"},
		{"myserver:2222", "myserver:2222"},
		{"192.168.1.1", "192.168.1.1:22"},
		{"192.168.1.1:22", "192.168.1.1:22"},
	}

	for _, tt := range tests {
		full := tt.input
		if !strings.Contains(tt.input, ":") {
			full = tt.input + ":22"
		}
		if full != tt.want {
			t.Errorf("port append for %q = %q, want %q", tt.input, full, tt.want)
		}
	}
}
