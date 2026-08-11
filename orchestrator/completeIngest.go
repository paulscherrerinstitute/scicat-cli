package orchestrator

import (
	"fmt"
	"log"
	"net/http"
	"path"
	"strings"
	"time"

	"github.com/paulscherrerinstitute/scicat-cli/v3/datasetIngestor"
	"github.com/paulscherrerinstitute/scicat-cli/v3/datasetUtils"
)

// The dependencies are assigned to module level vars so they can be swapped by mocks in tests
var getDatasetDetailsFunc = datasetUtils.GetDatasetDetails
var createOrigDatablocksFunc = datasetIngestor.CreateOrigDatablocks
var patchDatasetFunc = datasetUtils.PatchDataset
var markFilesReadyFunc = datasetIngestor.MarkFilesReady
var gatherCompletionFileListFunc = gatherCompletionFileList

/*
CompleteIngest defines and adds a dataset to the SciCat catalog for a dataset entry that was
previously created without any files attached (NumberOfFiles == 0).

It checks that the caller is allowed to perform the operation, that the dataset identified by
pid exists, is empty and has a sourceFolder defined, then gathers the local file list from that
sourceFolder and creates the corresponding origdatablocks. Symlinks are kept only when they point
internally to the sourceFolder; filenames containing "*", "\" or three consecutive blanks are
excluded from the dataset.
*/
func CompleteIngest(client *http.Client, APIServer string, user map[string]string, pid string, sourceFolderPrefix string) error {
	if err := requireArchiveManager(user, "complete the ingestion"); err != nil {
		return err
	}

	dataset, err := resolveEmptyDatasetSourceFolder(client, APIServer, user, pid)
	if err != nil {
		return err
	}

	log.Printf("Dataset with PID %s has sourceFolder %s\n", pid, dataset.SourceFolder)
	sourceFolder := dataset.SourceFolder
	if sourceFolderPrefix != "" {
		sourceFolder = path.Join(sourceFolderPrefix, sourceFolder)
		log.Printf("Using sourceFolder %s (prefix %s applied)\n", sourceFolder, sourceFolderPrefix)
	}

	fullFileArray, startTime, endTime, skippedLinks, illegalFileNames, err := gatherCompletionFileListFunc(sourceFolder)
	if err != nil {
		return err
	}

	if err := createOrigDatablocksFunc(client, APIServer, fullFileArray, pid, user); err != nil {
		return fmt.Errorf("failed to create origdatablocks for dataset %s: %w", pid, err)
	}

	if err := updateDatasetTimes(client, APIServer, user, pid, startTime, endTime); err != nil {
		return err
	}

	if err := markFilesReadyFunc(client, APIServer, pid, user); err != nil {
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

// requireArchiveManager enforces that only the archiveManager account may perform action.
// Kept as a pure function so the authorization rule can be unit-tested without any client/network setup.
func requireArchiveManager(user map[string]string, action string) error {
	if user["username"] != "archiveManager" {
		return fmt.Errorf("you must be archiveManager to be allowed to %s", action)
	}
	return nil
}

// resolveEmptyDatasetSourceFolder fetches the dataset identified by pid and validates that it is
// in the expected pre-completion state: it exists, has no files yet, and has a sourceFolder to
// scan. Returns that sourceFolder on success.
func resolveEmptyDatasetSourceFolder(client *http.Client, APIServer string, user map[string]string, pid string) (datasetUtils.Dataset, error) {
	dataset, missing, err := getDatasetDetailsFunc(client, APIServer, user["accessToken"], []string{pid}, "")
	if err != nil {
		return datasetUtils.Dataset{}, err
	}
	if len(missing) > 0 || len(dataset) != 1 {
		return datasetUtils.Dataset{}, fmt.Errorf("dataset with PID %s not found", pid)
	}
	if dataset[0].NumberOfFiles != 0 {
		return datasetUtils.Dataset{}, fmt.Errorf("dataset with PID %s already contains files", pid)
	}
	if dataset[0].SourceFolder == "" {
		return datasetUtils.Dataset{}, fmt.Errorf("dataset with PID %s has no sourceFolder defined", pid)
	}
	return dataset[0], nil
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
	return patchDatasetFunc(client, APIServer, user["accessToken"], pid, meta)
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
