package datasetUtils

import (
	"testing"
	"reflect"
	"os/exec"
	"github.com/stretchr/testify/assert"
)

func TestGetAvailableDatasets(t *testing.T) {
	// Mock getRsyncVersion
	getRsyncVersion = func() (string, error) {
		return "3.2.3", nil
	}
	
	// Test single dataset ID
	datasetID := "12345"
	expected := []string{"20.500.11935/" + datasetID}
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
		"20.500.11935/123456789012345678901234567890123456",
		"20.500.11935/987654321098765432109876543210987654",
	}
	actual := parseRsyncOutput(output)
	
	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("Expected %v, but got %v", expected, actual)
	}
}

func TestFormatDatasetId(t *testing.T) {
	datasetId := "20.500.11935/testId"
	expected := "20.500.11935/testId"
	actual := formatDatasetId(datasetId)
	
	if expected != actual {
		t.Errorf("Expected %s, but got %s", expected, actual)
	}
	
	datasetId = "testId"
	expected = "20.500.11935/testId"
	actual = formatDatasetId(datasetId)
	
	if expected != actual {
		t.Errorf("Expected %s, but got %s", expected, actual)
	}
}
