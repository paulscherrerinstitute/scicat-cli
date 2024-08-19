package cliutils

import (
	"log"
	"os"

	"github.com/paulscherrerinstitute/scicat/datasetIngestor"
)

func SshTransfer(params TransferParams) (archivable bool, err error) {
	// === collecting params. ===
	client := params.Client
	apiServer := params.ApiServer
	user := params.User
	rsyncServer := params.RsyncServer
	datasetId := params.DatasetId
	dsSourceFolder := params.DatasetSourceFolder
	absFilelistPath := params.AbsFilelistPath
	archivable = false

	// === copying files ===
	log.Println("Syncing files to cache server...")
	err = datasetIngestor.SyncLocalDataToFileserver(datasetId, user, rsyncServer, dsSourceFolder, absFilelistPath, os.Stdout)
	if err == nil {
		// mark dataset ready for archival
		archivable = true
		err = datasetIngestor.MarkFilesReady(client, apiServer, datasetId, user)
	}
	log.Println("Syncing files - DONE")

	return archivable, err
}
