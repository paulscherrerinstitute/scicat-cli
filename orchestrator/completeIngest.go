package orchestrator

import (
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/paulscherrerinstitute/scicat-cli/v3/datasetIngestor"
	"github.com/paulscherrerinstitute/scicat-cli/v3/datasetUtils"
)

/*
CompleteIngest defines and adds a dataset to the SciCat catalog for a dataset entry that was
previously created without any files attached (NumberOfFiles == 0).

It checks that the caller is allowed to perform the operation, that the dataset identified by
pid exists, is empty and has a sourceFolder defined, then gathers the local file list from that
sourceFolder and creates the corresponding origdatablocks. Symlinks are kept only when they point
internally to the sourceFolder; filenames containing "*", "\" or three consecutive blanks are
excluded from the dataset.
*/
func CompleteIngest(client *http.Client, APIServer string, user map[string]string, pid string) error {
	if err := requireArchiveManager(user); err != nil {
		return err
	}

	sourceFolder, err := resolveEmptyDatasetSourceFolder(client, APIServer, user, pid)
	if err != nil {
		return err
	}

	log.Printf("Dataset with PID %s has sourceFolder %s\n", pid, sourceFolder)

	fullFileArray, startTime, endTime, skippedLinks, illegalFileNames, err := gatherCompletionFileList(sourceFolder)
	if err != nil {
		return err
	}

	if err := datasetIngestor.CreateOrigDatablocks(client, APIServer, fullFileArray, pid, user); err != nil {
		return fmt.Errorf("failed to create origdatablocks for dataset %s: %w", pid, err)
	}

	if err := updateDatasetTimes(client, APIServer, user, pid, startTime, endTime); err != nil {
		return err
	}

	if err := datasetIngestor.MarkFilesReady(client, APIServer, pid, user); err != nil {
		return err
	}

	if skippedLinks > 0 {
		return &datasetIngestor.SkippedLinksWarning{Count: skippedLinks}
	}
	if illegalFileNames > 0 {
		return &datasetIngestor.IllegalFileNamesWarning{Count: illegalFileNames}
	}
	return nil
}

// requireArchiveManager enforces that only the archiveManager account may complete an ingest.
// Kept as a pure function so the authorization rule can be unit-tested without any client/network setup.
func requireArchiveManager(user map[string]string) error {
	if user["username"] != "archiveManager" {
		return fmt.Errorf("you must be archiveManager to be allowed to complete the ingestion")
	}
	return nil
}

// resolveEmptyDatasetSourceFolder fetches the dataset identified by pid and validates that it is
// in the expected pre-completion state: it exists, has no files yet, and has a sourceFolder to
// scan. Returns that sourceFolder on success.
func resolveEmptyDatasetSourceFolder(client *http.Client, APIServer string, user map[string]string, pid string) (string, error) {
	dataset, missing, err := datasetUtils.GetDatasetDetails(client, APIServer, user["accessToken"], []string{pid}, "")
	if err != nil {
		return "", err
	}
	if len(missing) > 0 || len(dataset) != 1 {
		return "", fmt.Errorf("dataset with PID %s not found", pid)
	}
	if dataset[0].NumberOfFiles != 0 {
		return "", fmt.Errorf("dataset with PID %s already contains files", pid)
	}
	if dataset[0].SourceFolder == "" {
		return "", fmt.Errorf("dataset with PID %s has no sourceFolder defined", pid)
	}
	return dataset[0].SourceFolder, nil
}

// gatherCompletionFileList scans sourceFolder and returns the resulting file list along with
// counts of symlinks skipped and files excluded for illegal filenames. Symlinks are kept only
// when they resolve to a path internal to sourceFolder ("dA" policy); this path never prompts,
// since dataset completion is meant to run unattended.
func gatherCompletionFileList(sourceFolder string) ([]datasetIngestor.Datafile, time.Time, time.Time, uint, uint, error) {
	skipSymlinks := "dA"
	var skippedLinks, illegalFileNames uint
	symlinkCallback := datasetIngestor.CreateLocalSymlinkCallbackForFileLister(&skipSymlinks, &skippedLinks)
	filenameFilterCallback := datasetIngestor.CreateLocalFilenameFilterCallback(&illegalFileNames)

	fullFileArray, startTime, endTime, _, _, _, err :=
		datasetIngestor.GetValidatedLocalFileList(sourceFolder, "", symlinkCallback, filenameFilterCallback)
	if err != nil {
		return nil, time.Time{}, time.Time{}, 0, 0, err
	}
	return fullFileArray, startTime, endTime, skippedLinks, illegalFileNames, nil
}

func updateDatasetTimes(client *http.Client, APIServer string, user map[string]string, pid string, startTime time.Time, endTime time.Time) error {
	meta := map[string]interface{}{
		"creationTime": startTime.Format(time.RFC3339),
		"endTime":      endTime.Format(time.RFC3339),
	}
	return datasetUtils.PatchDataset(client, APIServer, user["accessToken"], pid, meta)
}

func ExtractPidFromArgs(args []string) (string, error) {
	if len(args) != 1 {
		return "", fmt.Errorf("invalid number of args")
	}
	pid := args[0]
	if !strings.HasPrefix(pid, "20.500.11935/") {
		return "", fmt.Errorf("invalid pid, must start with 20.500.11935/")
	}
	return pid, nil
}
