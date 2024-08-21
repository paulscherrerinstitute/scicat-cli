package cliutils

import (
	"net/http"

	"github.com/SwissOpenEM/globus"
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
	DestCollection string
	Filelist       []string
	IsSymlinkList  []bool
}

type TransferParams struct {
	SshParams
	GlobusParams
	// other params
	DatasetId           string
	DatasetSourceFolder string
}
