package datasetUtils

import (
	"testing"
	"reflect"
	"os/exec"
	"os"
	"bytes"
	"io"
	"strings"
	"github.com/stretchr/testify/assert"
)

func TestGetAvailableDatasets(t *testing.T) {
	// Mock getRsyncVersion
	getRsyncVersion = func() (string, error) {
		return "3.2.3", nil
	}
	
	// Test single dataset ID
	datasetID := "12345"
	expected := []string{DatasetIdPrefix + "/" + datasetID}
	result, _ := GetAvailableDatasets("username", "rsyncserver", datasetID)
	assert.Equal(t, expected, result, "The two slices should be the same.")
}

func TestBuildRsyncCommand(t *testing.T) {
	username := "testUser"
	RSYNCServer := "testServer"
	versionNumber := "3.2.3"
	
	expectedCmd := exec.Command("rsync", "-e", "ssh", "--list-only", username+"@"+RSYNCServer+":retrieve/")
	actualCmd := buildRsyncCommand(username, RSYNCServer, versionNumber)
	
	// For slice comparison, a simple == operator won't work because Go does not allow it for slice types. So, reflect.DeepEqual is necessary.
	if !reflect.DeepEqual(expectedCmd, actualCmd) {
		t.Errorf("Expected command %v, but got %v", expectedCmd, actualCmd)
	}
	
	versionNumber = "3.2.2"
	expectedCmd = exec.Command("rsync", "-e", "ssh -q", "--list-only", username+"@"+RSYNCServer+":retrieve/")
	actualCmd = buildRsyncCommand(username, RSYNCServer, versionNumber)
	
	if !reflect.DeepEqual(expectedCmd, actualCmd) {
		t.Errorf("Expected command %v, but got %v", expectedCmd, actualCmd)
	}
}

func TestParseRsyncOutput(t *testing.T) {
	// The drwxr-xr-x string is a representation of file permissions in a Unix-like operating system
	output := []byte(`
	drwxr-xr-x          4,096 2022/01/01 01:01:01 123456789012345678901234567890123456
	drwxr-xr-x          4,096 2022/01/01 01:01:01 987654321098765432109876543210987654
	`)
	expected := []string{
		DatasetIdPrefix + "/123456789012345678901234567890123456",
		DatasetIdPrefix + "/987654321098765432109876543210987654",
	}
	actual := parseRsyncOutput(output)
	
	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("Expected %v, but got %v", expected, actual)
	}
}

func TestFormatDatasetId(t *testing.T) {
	datasetId := DatasetIdPrefix + "/testId"
	expected := DatasetIdPrefix + "/testId"
	actual := formatDatasetId(datasetId)
	
	if expected != actual {
		t.Errorf("Expected %s, but got %s", expected, actual)
	}
	
	datasetId = "testId"
	expected = DatasetIdPrefix + "/testId"
	actual = formatDatasetId(datasetId)
	
	if expected != actual {
		t.Errorf("Expected %s, but got %s", expected, actual)
	}
}

func TestPrintMessage(t *testing.T) {
	RSYNCServer := "testServer"
	
	expected := "\n\n\n====== Checking for available datasets on archive cache server testServer:\n"
	expected += "====== (only datasets highlighted in green will be retrieved)\n\n"
	expected += "====== If you can not find the dataset in this listing: may be you forgot\n"
	expected += "====== to start the necessary retrieve job from the the data catalog first?\n\n"
	
	// Redirect standard output to a buffer
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w
	
	printMessage(RSYNCServer)
	
	// Restore standard output
	w.Close()
	os.Stdout = old
	
	var buf bytes.Buffer
	io.Copy(&buf, r)
	actual := buf.String()
	
	if !strings.EqualFold(expected, actual) {
		t.Errorf("Expected %s, but got %s", expected, actual)
	}
}
