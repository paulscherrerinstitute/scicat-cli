package orchestrator

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/paulscherrerinstitute/scicat-cli/v3/datasetIngestor"
)

// PrepareDataset scans a dataset's local files via datasetIngestor.GetValidatedLocalFileList and,
// if the dataset survives the empty/too-many-files checks, updates and logs its metadata.
//
// The returned error follows the same errors.As pattern as ResolveCentralAvailability:
// *datasetIngestor.EmptyDatasetError or *datasetIngestor.TooManyFilesError just mean this dataset
// must be skipped (not fatal, no os.Exit); anything else is a hard failure gathering the local
// file list. emptyDatasets/tooLargeDatasets are incremented to match whichever of those two
// errors is returned.
func PrepareDataset(client *http.Client, APIServer string, user map[string]string,
	originalMap map[string]string, metaDataMap map[string]interface{}, tapecopies int,
	datasetSourceFolder string, datasetFileListTxt string,
	symlinkCallback func(symlinkPath string, sourceFolder string) (bool, error),
	filenameCheckCallback func(filepath string) bool,
	emptyDatasets *int, tooLargeDatasets *int) (fullFileArray []datasetIngestor.Datafile, err error) {
	fullFileArray, startTime, endTime, owner, numFiles, totalSize, err :=
		datasetIngestor.GetValidatedLocalFileList(datasetSourceFolder, datasetFileListTxt, symlinkCallback, filenameCheckCallback)
	if err != nil {
		var emptyDatasetErr *datasetIngestor.EmptyDatasetError
		var tooManyFilesErr *datasetIngestor.TooManyFilesError
		switch {
		case errors.As(err, &emptyDatasetErr):
			(*emptyDatasets)++
		case errors.As(err, &tooManyFilesErr):
			(*tooLargeDatasets)++
		}
		return fullFileArray, err
	}
	log.Println("File list collected.")
	log.Printf("The dataset contains %v files with a total size of %v bytes.\n", numFiles, totalSize)

	UpdateAndLogMetaData(client, APIServer, user, originalMap, metaDataMap, startTime, endTime, owner, tapecopies)
	return fullFileArray, nil
}

// UpdateAndLogMetaData updates the dataset's metadata fields from the
// scanned file list and logs the resulting metadata object.
func UpdateAndLogMetaData(client *http.Client, APIServer string, user map[string]string,
	originalMap map[string]string, metaDataMap map[string]interface{}, startTime time.Time, endTime time.Time, owner string, tapecopies int) {
	datasetIngestor.UpdateMetaData(client, APIServer, user, originalMap, metaDataMap, startTime, endTime, owner, tapecopies)
	pretty, _ := json.MarshalIndent(metaDataMap, "", "    ")
	log.Printf("Updated metadata object:\n%s\n", pretty)
}

// ErrCopyRequiresPersonalAccount is returned when the data is not centrally
// available (and therefore needs to be copied) but no access group was
// provided, i.e. a beamline account is used instead of a personal one.
var ErrCopyRequiresPersonalAccount = errors.New("for copying, you must use a personal account. Beamline accounts are not supported.")

// ErrIngestAborted is returned when the caller declines to continue after
// being told that the data is not centrally available and must be copied.
var ErrIngestAborted = errors.New("further ingests interrupted because copying is needed, but no copy wanted.")

// NotCentrallyAvailableWarning reports, as a non-fatal warning, that a dataset's sourceFolder is
// not centrally available and must be copied before it can be archived.
type NotCentrallyAvailableWarning struct {
	SourceFolder string
}

func (w *NotCentrallyAvailableWarning) Error() string {
	return fmt.Sprintf("The source folder %v is not centrally available.\nThe data must first be copied.\n ", w.SourceFolder)
}

// ResolveCentralAvailability checks whether the dataset's source folder is available on the
// central archive server via SSH, and decides the resulting copyFlag (currentCopyFlag is returned
// unchanged when the data is centrally available).
//
// When the data is not centrally available, copying is required: on success (noninteractive, or
// the user accepted via confirmContinue) it returns copyFlag=true alongside a
// *NotCentrallyAvailableWarning - not a failure, just something the caller should report. It
// returns ErrCopyRequiresPersonalAccount if no personal account (access group) is available, and
// ErrIngestAborted if the user declines to continue.
func ResolveCentralAvailability(username string, rsyncServer string, datasetSourceFolder string, sshOutput io.Writer,
	currentCopyFlag bool, accessGroups []string, noninteractive bool, confirmContinue func() bool) (copyFlag bool, err error) {
	log.Println("Checking if data is centrally available...")
	sshErr, otherErr := datasetIngestor.CheckDataCentrallyAvailableSsh(username, rsyncServer, datasetSourceFolder, sshOutput)
	if otherErr != nil {
		return currentCopyFlag, fmt.Errorf("cannot check if data is centrally available: %w", otherErr)
	}
	if sshErr == nil {
		log.Println("Data is present centrally.")
		return currentCopyFlag, nil
	}

	if len(accessGroups) == 0 {
		return false, ErrCopyRequiresPersonalAccount
	}
	if !noninteractive && confirmContinue != nil && !confirmContinue() {
		return false, ErrIngestAborted
	}
	return true, &NotCentrallyAvailableWarning{SourceFolder: datasetSourceFolder}
}
