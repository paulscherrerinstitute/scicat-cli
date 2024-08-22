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
	srcPrefixPath := params.SrcPrefixPath
	dsSourceFolder := params.DatasetSourceFolder

	destCollection := params.DestCollection
	destPrefixPath := params.DestPrefixPath
	datasetId := params.DatasetId

	archivable = false // the dataset is never archivable after a globus transfer request immediately
	destFolder := destPrefixPath + "/archive/" + strings.Split(datasetId, "/")[1] + dsSourceFolder

	for i := range fileList {
		fileList[i] = srcPrefixPath + "/" + fileList[i]
	}

	// === copying files ===
	log.Println("Syncing files to cache server...")
	result, err := globusClient.TransferFileList(srcCollection, dsSourceFolder, destCollection, destFolder, fileList, isSymlinkList, true)
	log.Printf("The transfer result response: \n=====\n")
	log.Printf("Task ID: %s\n", result.TaskId)
	log.Printf("Code: %s\n", result.SubmissionId)
	log.Printf("Message: %s\n", result.Message)
	log.Printf("Resource: %s\n", result.Resource)
	log.Printf("=====\n")
	log.Println("Syncing files - STARTED")

	// === return results ===
	return archivable, err
}
