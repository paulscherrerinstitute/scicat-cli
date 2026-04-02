package cliutils

import (
	"fmt"
	"log"
	"net/http"

	"github.com/paulscherrerinstitute/scicat-cli/v3/datasetUtils"
)

// IngestorConfig holds all the setup parameters
type IngestorConfig struct {
	Userpass          string
	Token             string
	ScicatUrl         string
	Testenv           bool
	Devenv            bool
	Oidc              bool
	RequireArchiveMgr bool
	NonInteractive    bool
	RemoveFromCatalog bool
	PID               string
	DeletionCode      string
	DeletionReason    string
}

func RunDeletion(client *http.Client, cfg IngestorConfig, version string, cmdName string) error {
	// 1. Version and Availability Checks
	datasetUtils.CheckForNewVersion(client, cmdName, version)
	datasetUtils.CheckForServiceAvailability(client, cfg.Testenv, true)

	// 2. Resolve API Server
	config := InputEnvironmentConfig{
		TestenvFlag: cfg.Testenv,
		DevenvFlag:  cfg.Devenv,
		ScicatUrl:   cfg.ScicatUrl,
	}

	apiServer := config.ResolveAPIServer()

	// 3. Authenticate
	user, _, err := Authenticate(RealAuthenticator{}, client, apiServer, cfg.Userpass, cfg.Token, cfg.Oidc)
	if err != nil {
		return err
	}

	if cfg.RequireArchiveMgr && user["username"] != "archiveManager" {
		return fmt.Errorf("permission denied: must be archiveManager")
	}

	// 4. Execution Logic
	log.Printf("Starting Archive Removal for PID: %s", cfg.PID)
	jobID, err := datasetUtils.RemoveFromArchive(client, apiServer, cfg.PID, user, cfg.NonInteractive, datasetUtils.JobParamsStruct{
		DeletionCode:   datasetUtils.DeletionCode(cfg.DeletionCode),
		DeletionReason: cfg.DeletionReason,
	})
	if err != nil {
		if jobID != "" {
			datasetUtils.PatchJobStatus(client, apiServer, user, jobID, string(datasetUtils.JobFailed))
		}
		return err
	}

	// 5. Optional Catalog Removal
	if cfg.RemoveFromCatalog {
		err = datasetUtils.RemoveFromCatalog(client, apiServer, cfg.PID, jobID, user, cfg.NonInteractive)
		if err != nil {
			datasetUtils.PatchJobStatus(client, apiServer, user, jobID, string(datasetUtils.JobFailed))
			return err
		}
	}

	return nil
}
