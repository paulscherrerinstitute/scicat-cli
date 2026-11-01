package orchestrator

import (
	"log"
	"net/http"

	"github.com/paulscherrerinstitute/scicat-cli/v3/datasetUtils"
)

// The dependencies are assigned to module level vars so they can be swapped by mocks in tests
var removeFromArchiveFunc = datasetUtils.RemoveFromArchive
var removeFromCatalogFunc = datasetUtils.RemoveFromCatalog
var patchJobStatusFunc = datasetUtils.PatchJobStatus

/*
CleanDataset removes a dataset from the archive and, optionally, from the data catalog.

It checks that the caller is allowed to perform the operation, then launches a reset job to
remove any Datablock entries for pid from the archive system. If removeFromCatalog is true, it
then also removes the dataset's catalog entries (Dataset and OrigDatablock); this only happens
once the reset job has finished. If either step fails, the reset job is patched to a failed status.
*/
func CleanDataset(client *http.Client, APIServer string, user map[string]string, pid string, nonInteractive bool, removeFromCatalog bool, opts datasetUtils.RemovalJobOptions) error {
	if err := datasetUtils.RequireArchiveManager(user, "delete datasets"); err != nil {
		return err
	}

	jobID, err := removeFromArchiveFunc(client, APIServer, pid, user, nonInteractive, opts)
	if err != nil {
		failJob(client, APIServer, user, jobID)
		return err
	}

	if !removeFromCatalog {
		log.Println("To also delete the dataset from the catalog add the flag --removeFromCatalog")
		return nil
	}

	if err := removeFromCatalogFunc(client, APIServer, pid, jobID, user, nonInteractive); err != nil {
		failJob(client, APIServer, user, jobID)
		return err
	}

	return nil
}

// failJob best-effort patches jobID to a failed status; jobID is empty when no job was ever
// created (e.g. the dataset had no datablocks to remove), in which case there is nothing to patch.
func failJob(client *http.Client, APIServer string, user map[string]string, jobID string) {
	if jobID == "" {
		return
	}
	if err := patchJobStatusFunc(client, APIServer, user, jobID, string(datasetUtils.JobFailed)); err != nil {
		log.Printf("Failed to patch job status: %v", err)
	}
}
