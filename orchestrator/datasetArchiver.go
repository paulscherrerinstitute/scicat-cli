package orchestrator

import (
	"fmt"
	"net/http"
	"time"

	"github.com/paulscherrerinstitute/scicat-cli/v3/datasetUtils"
)

/*
ResolveArchivableDatasets returns the list of dataset PIDs to submit for archiving: the archivable
datasets of ownerGroup, optionally narrowed down to inputDatasetList. ownerGroup is
optional, but if it is empty, inputDatasetList must be set, and every one of its datasetIds must
resolve to an existing, archivable dataset (an error is returned otherwise). Callers typically
enforce beforehand (e.g. via a CLI flag/positional-args check) that at least one of
ownerGroup/inputDatasetList is set; this function still reports a descriptive error if neither
is set.
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
