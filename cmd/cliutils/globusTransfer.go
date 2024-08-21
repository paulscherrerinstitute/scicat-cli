package cliutils

import (
	"log"
	"strings"
)

func GlobusTransfer(params TransferParams) (archivable bool, err error) {
	// === collecting params. ===
	globusClient := params.GlobusClient

	fileList := params.Filelist
	isSymlinkList := params.IsSymlinkList
	srcCollection := params.SrcCollection
	dsSourceFolder := params.DatasetSourceFolder

	destCollection := params.DestCollection
	datasetId := params.DatasetId

	archivable = false // the dataset is never archivable after a globus transfer request immediately
	destFolder := "archive/" + strings.Split(datasetId, "/")[1] + dsSourceFolder

	// === copying files ===
	log.Println("Syncing files to cache server...")
	result, err := globusClient.TransferFileList(srcCollection, dsSourceFolder, destCollection, destFolder, fileList, isSymlinkList, true)
	log.Printf("The transfer result response: \n=====\n%v\n=====\n", result)
	log.Println("Syncing files - STARTED")

	// === return results ===
	return archivable, err
}
