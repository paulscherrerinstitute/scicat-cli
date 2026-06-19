package backend

import (
	"log"

	"github.com/fatih/color"
	"github.com/paulscherrerinstitute/scicat-cli/v3/datasetUtils"
)

// ArchiveService instantiates the archive domain worker module.
type ArchiveService struct {
	Base *TransportEngine
}

// NewArchiveService creates an instance of the archive service attached to the base transport.
func NewArchiveService(base *TransportEngine) *ArchiveService {
	return &ArchiveService{Base: base}
}

// SubmitArchivalJob registers a discrete archival tape backup request with SciCat.
func (as *ArchiveService) SubmitArchivalJob(commonOwnerGroup string, datasetIDs []string, tapeCopies int) (string, error) {
	if len(datasetIDs) == 0 {
		log.Println("[ArchiveService] Skipped job creation: No matching dataset IDs provided.")
		return "", nil
	}

	log.Printf("[ArchiveService] Submitting archival job for %d datasets under group '%s'...\n", len(datasetIDs), commonOwnerGroup)

	// Delegate task execution down to your legacy shared system library utilities cleanly
	jobId, err := datasetUtils.CreateArchivalJob(
		as.Base.Client,
		as.Base.APIServer,
		as.Base.UserSession.User,
		commonOwnerGroup,
		datasetIDs,
		&tapeCopies,
		nil,
	)
	if err != nil {
		color.Set(color.FgRed)
		log.Printf("[ArchiveService] Archival job submission failed: %s\n", err.Error())
		color.Unset()
		return "", err
	}

	color.Set(color.FgGreen)
	log.Println("[ArchiveService] Submitted job successfully! Job ID:", jobId)
	color.Unset()

	return jobId, nil
}
