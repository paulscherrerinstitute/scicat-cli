package orchestrator

import (
	"fmt"
	"net/http"
	"time"

	"github.com/paulscherrerinstitute/scicat-cli/v3/datasetUtils"
)

/*
ResolveArchivableDatasets returns the list of dataset PIDs to submit for archiving.

If inputDatasetList is non-empty, it returns the archivable datasets matching those
PIDs that the user's accessToken can access (ownerGroup is ignored); if any PID is
missing, not archivable, or not accessible, it returns an error.

Otherwise, it returns the archivable datasets belonging to ownerGroup. If neither
ownerGroup nor inputDatasetList is set, it returns an error.
*/
func ResolveArchivableDatasets(client *http.Client, APIServer string, accessToken string, ownerGroup string, inputDatasetList []string) ([]string, error) {
	if ownerGroup == "" && len(inputDatasetList) == 0 {
		return nil, fmt.Errorf("either ownergroup or datasetId(s) must be specified")
	}

	archivableDatasets, err := datasetUtils.GetArchivableDatasets(client, APIServer, ownerGroup, inputDatasetList, accessToken)
	if err != nil {
		return nil, fmt.Errorf("GetArchivableDatasets: %w", err)
	}

	if len(inputDatasetList) > 0 && len(archivableDatasets) != len(inputDatasetList) {
		return nil, fmt.Errorf("some datasetIds are missing or not archivable")
	}

	if len(archivableDatasets) == 0 {
		return nil, fmt.Errorf("no archivable datasets remaining")
	}
	return archivableDatasets, nil
}

// ParseExecutionTime parses s as an RFC3339 timestamp, e.g. as read from a CLI flag. It returns
// (nil, nil) when s is empty, so callers can pass the result straight to CreateArchivalJob.
func ParseExecutionTime(s string) (*time.Time, error) {
	if s == "" {
		return nil, nil
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return nil, fmt.Errorf("execution time is invalid: %w", err)
	}
	return &t, nil
}

func ResolveOwnerGroup(ownerGroup string, accessGroups []string) (string, error) {
	if ownerGroup != "" {
		return ownerGroup, nil
	}
	if len(accessGroups) == 0 {
		return "", fmt.Errorf("Could not determine an ownerGroup to submit the archive job for: specify --ownergroup or ensure your account has at least one access group")
	}
	return accessGroups[0], nil
}
