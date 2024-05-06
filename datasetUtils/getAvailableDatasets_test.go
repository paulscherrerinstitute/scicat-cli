package datasetUtils

import (
	"testing"
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
	result := GetAvailableDatasets("username", "rsyncserver", datasetID)
	assert.Equal(t, expected, result, "The two slices should be the same.")
}
