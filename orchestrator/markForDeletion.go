package orchestrator

import (
	"net/http"

	"github.com/paulscherrerinstitute/scicat-cli/v3/datasetUtils"
)

/*
MarkDatasetForDeletion flags a dataset's archived data for deletion by launching a
markForDeletion job carrying a deletion code and reason.

Unlike CleanDataset, it is open to any authenticated user and never touches the data catalog -
it only records the request against the archive system.
*/
func MarkDatasetForDeletion(client *http.Client, APIServer string, user map[string]string, pid string, nonInteractive bool, opts datasetUtils.RemovalJobOptions) error {
	_, err := submitRemovalJob(client, APIServer, user, pid, nonInteractive, datasetUtils.JobTypeMarkForDeletion, opts)
	return err
}
