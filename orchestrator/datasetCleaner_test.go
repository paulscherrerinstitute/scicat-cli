package orchestrator

import (
	"errors"
	"net/http"
	"testing"

	"github.com/paulscherrerinstitute/scicat-cli/v3/datasetUtils"
)

// withCleanDatasetMocks installs default (successful, no-op) mocks for all of CleanDataset's
// dependencies and restores the originals on test cleanup. Each test then only needs to override
// whichever dependency it actually cares about.
func withCleanDatasetMocks(t *testing.T) {
	t.Helper()
	oldRemoveFromArchive := removeFromArchiveFunc
	oldRemoveFromCatalog := removeFromCatalogFunc
	oldPatchJobStatus := patchJobStatusFunc
	t.Cleanup(func() {
		removeFromArchiveFunc = oldRemoveFromArchive
		removeFromCatalogFunc = oldRemoveFromCatalog
		patchJobStatusFunc = oldPatchJobStatus
	})

	removeFromArchiveFunc = func(client *http.Client, APIServer string, pid string, user map[string]string, nonInteractive bool, jobType string, opts datasetUtils.RemovalJobOptions) (string, error) {
		return "job1", nil
	}
	removeFromCatalogFunc = func(client *http.Client, APIServer string, pid string, jobID string, user map[string]string, nonInteractive bool) error {
		return nil
	}
	patchJobStatusFunc = func(client *http.Client, APIServer string, user map[string]string, jobID string, status string) error {
		return nil
	}
}

func TestCleanDataset(t *testing.T) {
	archiveManager := map[string]string{"username": "archiveManager", "accessToken": "testToken"}

	t.Run("rejects non archiveManager users", func(t *testing.T) {
		err := CleanDataset(nil, "", map[string]string{"username": "someoneElse"}, "testPid", true, false, datasetUtils.RemovalJobOptions{})
		if err == nil {
			t.Fatal("expected an error, got nil")
		}
	})

	t.Run("removes the dataset from the archive and leaves the catalog untouched by default", func(t *testing.T) {
		withCleanDatasetMocks(t)
		var catalogCalled bool
		removeFromCatalogFunc = func(client *http.Client, APIServer string, pid string, jobID string, user map[string]string, nonInteractive bool) error {
			catalogCalled = true
			return nil
		}

		if err := CleanDataset(nil, "", archiveManager, "testPid", true, false, datasetUtils.RemovalJobOptions{}); err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if catalogCalled {
			t.Error("expected the catalog not to be touched when removeFromCatalog is false")
		}
	})

	t.Run("also removes the dataset from the catalog when removeFromCatalog is true", func(t *testing.T) {
		withCleanDatasetMocks(t)
		var gotPid, gotJobID string
		removeFromCatalogFunc = func(client *http.Client, APIServer string, pid string, jobID string, user map[string]string, nonInteractive bool) error {
			gotPid = pid
			gotJobID = jobID
			return nil
		}

		if err := CleanDataset(nil, "", archiveManager, "testPid", true, true, datasetUtils.RemovalJobOptions{}); err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if gotPid != "testPid" || gotJobID != "job1" {
			t.Errorf("expected catalog removal for pid %q job %q, got pid %q job %q", "testPid", "job1", gotPid, gotJobID)
		}
	})

	t.Run("submits a reset job carrying the deletion code and reason", func(t *testing.T) {
		withCleanDatasetMocks(t)
		var gotJobType string
		var gotOpts datasetUtils.RemovalJobOptions
		removeFromArchiveFunc = func(client *http.Client, APIServer string, pid string, user map[string]string, nonInteractive bool, jobType string, opts datasetUtils.RemovalJobOptions) (string, error) {
			gotJobType = jobType
			gotOpts = opts
			return "job1", nil
		}
		wantOpts := datasetUtils.RemovalJobOptions{DeletionCode: "SUPERSEDED", DeletionReason: "replaced by a newer dataset"}

		if err := CleanDataset(nil, "", archiveManager, "testPid", true, false, wantOpts); err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if gotJobType != datasetUtils.JobTypeReset {
			t.Errorf("jobType = %q, want %q", gotJobType, datasetUtils.JobTypeReset)
		}
		if gotOpts != wantOpts {
			t.Errorf("RemovalJobOptions = %+v, want %+v", gotOpts, wantOpts)
		}
	})

	t.Run("patches the job to failed and returns the error when archive removal fails", func(t *testing.T) {
		withCleanDatasetMocks(t)
		removeFromArchiveFunc = func(client *http.Client, APIServer string, pid string, user map[string]string, nonInteractive bool, jobType string, opts datasetUtils.RemovalJobOptions) (string, error) {
			return "job1", errors.New("boom")
		}
		var patchedJobID, patchedStatus string
		patchJobStatusFunc = func(client *http.Client, APIServer string, user map[string]string, jobID string, status string) error {
			patchedJobID = jobID
			patchedStatus = status
			return nil
		}

		err := CleanDataset(nil, "", archiveManager, "testPid", true, true, datasetUtils.RemovalJobOptions{})
		if err == nil {
			t.Fatal("expected an error, got nil")
		}
		if patchedJobID != "job1" || patchedStatus != string(datasetUtils.JobFailed) {
			t.Errorf("expected job1 to be patched to %q, got job %q status %q", datasetUtils.JobFailed, patchedJobID, patchedStatus)
		}
	})

	t.Run("does not patch the job when archive removal fails without creating one", func(t *testing.T) {
		withCleanDatasetMocks(t)
		removeFromArchiveFunc = func(client *http.Client, APIServer string, pid string, user map[string]string, nonInteractive bool, jobType string, opts datasetUtils.RemovalJobOptions) (string, error) {
			return "", errors.New("boom")
		}
		var patchCalled bool
		patchJobStatusFunc = func(client *http.Client, APIServer string, user map[string]string, jobID string, status string) error {
			patchCalled = true
			return nil
		}

		if err := CleanDataset(nil, "", archiveManager, "testPid", true, true, datasetUtils.RemovalJobOptions{}); err == nil {
			t.Fatal("expected an error, got nil")
		}
		if patchCalled {
			t.Error("expected no patch call when no job was created")
		}
	})

	t.Run("patches the job to failed and returns the error when catalog removal fails", func(t *testing.T) {
		withCleanDatasetMocks(t)
		removeFromCatalogFunc = func(client *http.Client, APIServer string, pid string, jobID string, user map[string]string, nonInteractive bool) error {
			return errors.New("boom")
		}
		var patchedJobID, patchedStatus string
		patchJobStatusFunc = func(client *http.Client, APIServer string, user map[string]string, jobID string, status string) error {
			patchedJobID = jobID
			patchedStatus = status
			return nil
		}

		err := CleanDataset(nil, "", archiveManager, "testPid", true, true, datasetUtils.RemovalJobOptions{})
		if err == nil {
			t.Fatal("expected an error, got nil")
		}
		if patchedJobID != "job1" || patchedStatus != string(datasetUtils.JobFailed) {
			t.Errorf("expected job1 to be patched to %q, got job %q status %q", datasetUtils.JobFailed, patchedJobID, patchedStatus)
		}
	})
}
