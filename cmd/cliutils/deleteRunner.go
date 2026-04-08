package cliutils

import (
	"fmt"
	"log"
	"net/http"

	"github.com/fatih/color"
	"github.com/paulscherrerinstitute/scicat-cli/v3/datasetUtils"
)

type BaseConfig struct {
	EnvConfig      InputEnvironmentConfig
	Userpass       string
	Token          string
	Oidc           bool
	NonInteractive bool
	HttpClient     *http.Client

	apiServer string
	user      map[string]string
}

type CleanConfig struct {
	BaseConfig
	RemoveFromCatalog bool
}

type RemoveConfig struct {
	BaseConfig
	DeletionCode   string
	DeletionReason string
}

// authenticate resolves the environment and logs the user in.
func (c *BaseConfig) authenticate() error {
	// Resolve API Server
	envConfig := InputEnvironmentConfig{
		TestenvFlag: c.EnvConfig.TestenvFlag,
		DevenvFlag:  c.EnvConfig.DevenvFlag,
		ScicatUrl:   c.EnvConfig.ScicatUrl,
	}
	c.apiServer = envConfig.ResolveAPIServer()

	// Authenticate
	user, _, err := Authenticate(RealAuthenticator{}, c.HttpClient, c.apiServer, c.Userpass, c.Token, c.Oidc)
	if err != nil {
		return err
	}
	c.user = user
	return nil
}

// runCommonDeletion handles version checks and the core archive removal call.
func (c *BaseConfig) runCommonDeletion(pid string, version, cmdName string, params datasetUtils.JobParamsStruct) (string, error) {
	// Version and Availability Checks
	datasetUtils.CheckForNewVersion(c.HttpClient, cmdName, version)
	datasetUtils.CheckForServiceAvailability(c.HttpClient, c.EnvConfig.TestenvFlag, true)

	log.Printf("Starting Archive Removal for PID: %s", pid)

	// Execute core archive removal
	jobID, err := datasetUtils.RemoveFromArchive(c.HttpClient, c.apiServer, pid, c.user, c.NonInteractive, params)
	if err != nil {
		if jobID != "" {
			datasetUtils.PatchJobStatus(c.HttpClient, c.apiServer, c.user, jobID, string(datasetUtils.JobFailed))
		}
		return "", err
	}

	return jobID, err
}

// --- 2. Exposed Methods ---

// RunFullRemoval handles Auth -> User Check -> Archive Removal -> Catalog Removal.
func (c *CleanConfig) RunFullRemoval(pid string, version, cmdName string) error {
	// 1. Authenticate and verify user
	err := c.authenticate()
	if err != nil {
		return err
	}
	// Immediate User Check
	if c.user["username"] != "archiveManager" {
		return fmt.Errorf("permission denied: must be archiveManager")
	}

	// 2. Run the deletion engine
	jobID, err := c.runCommonDeletion(pid, version, cmdName, datasetUtils.JobParamsStruct{})
	if err != nil {
		return err
	}

	if c.RemoveFromCatalog {
		err = datasetUtils.RemoveFromCatalog(c.HttpClient, c.apiServer, pid, jobID, c.user, c.NonInteractive)
		if err != nil {
			datasetUtils.PatchJobStatus(c.HttpClient, c.apiServer, c.user, jobID, string(datasetUtils.JobFailed))
			return err
		}
	}

	color.Cyan("Full removal process completed successfully.")
	return nil
}

// RunArchiveOnlyRemoval triggers the flow without the catalog step.
func (c *RemoveConfig) RunArchiveOnlyRemoval(pid string, version, cmdName string) error {
	// 1. Authenticate and verify user
	err := c.authenticate()
	if err != nil {
		return err
	}

	// 2. Run the deletion engine
	jobID, err := c.runCommonDeletion(pid, version, cmdName, datasetUtils.JobParamsStruct{
		DeletionCode:   datasetUtils.DeletionCode(c.DeletionCode),
		DeletionReason: c.DeletionReason,
	})
	if err != nil {
		return err
	}
	color.Cyan("Archive removal job submitted (JobID: %s).", jobID)
	return nil
}
