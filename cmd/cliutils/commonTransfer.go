package cliutils

import (
	"net/http"

	"github.com/SwissOpenEM/globus"
	"github.com/paulscherrerinstitute/scicat-cli/v3/datasetUtils"
)

type SshParams struct {
	Client          *http.Client
	ApiServer       string
	User            map[string]string
	RsyncServer     string
	AbsFilelistPath string
}

type GlobusParams struct {
	GlobusClient   globus.GlobusClient
	SrcCollection  string
	SrcPrefixPath  string
	DestCollection string
	DestPrefixPath string
	Filelist       []string
	IsSymlinkList  []bool
}

type TransferParams struct {
	SshParams
	GlobusParams
	// other params
	DatasetId            string
	DatasetSourceFolder  string
	ArchiveStatusMessage datasetUtils.ArchiveStatusMessage
}
