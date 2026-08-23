package orchestrator

import (
	"errors"
	"net/http"
	"testing"

	"github.com/paulscherrerinstitute/scicat-cli/v3/datasetUtils"
)

func TestMarkDatasetForDeletion(t *testing.T) {
	anyUser := map[string]string{"username": "someoneElse", "accessToken": "testToken"}

	t.Run("is allowed for a non archiveManager user", func(t *testing.T) {
		withCleanDatasetMocks(t)

		if err := MarkDatasetForDeletion(nil, "", anyUser, "testPid", true, datasetUtils.RemovalJobOptions{}); err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
	})

	t.Run("submits a markForDeletion job carrying the deletion code and reason", func(t *testing.T) {
		withCleanDatasetMocks(t)
		var gotJobType string
		var gotOpts datasetUtils.RemovalJobOptions
		removeFromArchiveFunc = func(client *http.Client, APIServer string, pid string, user map[string]string, nonInteractive bool, jobType string, opts datasetUtils.RemovalJobOptions) (string, error) {
			gotJobType = jobType
			gotOpts = opts
			return "job1", nil
		}
		wantOpts := datasetUtils.RemovalJobOptions{DeletionCode: "EXPIRED", DeletionReason: "retention elapsed"}

		if err := MarkDatasetForDeletion(nil, "", anyUser, "testPid", true, wantOpts); err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if gotJobType != datasetUtils.JobTypeMarkForDeletion {
			t.Errorf("jobType = %q, want %q", gotJobType, datasetUtils.JobTypeMarkForDeletion)
		}
		if gotOpts != wantOpts {
			t.Errorf("RemovalJobOptions = %+v, want %+v", gotOpts, wantOpts)
		}
	})

	t.Run("never touches the data catalog", func(t *testing.T) {
		withCleanDatasetMocks(t)
		var catalogCalled bool
		removeFromCatalogFunc = func(client *http.Client, APIServer string, pid string, jobID string, user map[string]string, nonInteractive bool) error {
			catalogCalled = true
			return nil
		}

		if err := MarkDatasetForDeletion(nil, "", anyUser, "testPid", true, datasetUtils.RemovalJobOptions{}); err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if catalogCalled {
			t.Error("expected the catalog never to be touched")
		}
	})

	t.Run("patches the job to failed and returns the error when job submission fails", func(t *testing.T) {
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

		err := MarkDatasetForDeletion(nil, "", anyUser, "testPid", true, datasetUtils.RemovalJobOptions{})
		if err == nil {
			t.Fatal("expected an error, got nil")
		}
		if patchedJobID != "job1" || patchedStatus != string(datasetUtils.JobFailed) {
			t.Errorf("expected job1 to be patched to %q, got job %q status %q", datasetUtils.JobFailed, patchedJobID, patchedStatus)
		}
	})
}
