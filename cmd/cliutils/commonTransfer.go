package cliutils

import (
	"net/http"

	"github.com/SwissOpenEM/globus"
)

type TransferParams struct {
	// ssh transfer
	Client          *http.Client
	ApiServer       string
	User            map[string]string
	RsyncServer     string
	AbsFilelistPath string
	// globus transfer
	GlobusClient   globus.GlobusClient
	SrcCollection  string
	DestCollection string
	Filelist       []string
	// dataset params
	DatasetId           string
	DatasetSourceFolder string
}
