package core

import (
	"context"
	"crypto/tls"
	"errors"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/paulscherrerinstitute/scicat/datasetIngestor"
)

func IngestDataset(
	task_context context.Context,
	app_context context.Context,
	task IngestionTask,
) (string, error) {

	var http_client = &http.Client{
		Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}},
		Timeout:   120 * time.Second}

	SCIAT_API_URL := task.ScicatUrl

	const TAPECOPIES = 2 // dummy value, unused
	const DATASETFILELISTTXT = ""

	var skipSymlinks string = "dA" // skip all simlinks
	var skippedLinks uint = 0      // output variables
	var localSymlinkCallback = createLocalSymlinkCallbackForFileLister(&skipSymlinks, &skippedLinks)

	var illegalFileNames uint = 0
	var localFilepathFilterCallback = createLocalFilenameFilterCallback(&illegalFileNames)

	user := map[string]string{
		"accessToken": task.ScicatAccessToken,
	}
	// check if dataset already exists (identified by source folder)
	metadatafile := filepath.Join(task.DatasetFolder.FolderPath, "metadata.json")
	if _, err := os.Stat(metadatafile); errors.Is(err, os.ErrNotExist) {
		// path/to/whatever does not exist
		return "", err
	}

	accessGroups := []string{}

	newMetaDataMap, metadataSourceFolder, _, err := datasetIngestor.ReadAndCheckMetadata(http_client, SCIAT_API_URL, metadatafile, user, accessGroups)

	_ = metadataSourceFolder
	if err != nil {
		// log.Fatal("Error in CheckMetadata function: ", err)
		return "", err
	}

	// collect (local) files

	fullFileArray, startTime, endTime, owner, numFiles, totalSize, err := datasetIngestor.GetLocalFileList(task.DatasetFolder.FolderPath, DATASETFILELISTTXT, localSymlinkCallback, localFilepathFilterCallback)
	_ = numFiles
	_ = totalSize
	_ = startTime
	_ = endTime
	_ = owner
	if err != nil {
		log.Printf("")
		return "", err
	}
	originalMetaDataMap := map[string]string{}
	datasetIngestor.UpdateMetaData(http_client, SCIAT_API_URL, user, originalMetaDataMap, newMetaDataMap, startTime, endTime, owner, TAPECOPIES)
	// pretty, _ := json.MarshalIndent(newMetaDataMap, "", "    ")

	// log.Printf("Updated metadata object:\n%s\n", pretty)
	// ingest dataset -> returns PID

	newMetaDataMap["datasetlifecycle"] = map[string]interface{}{}
	newMetaDataMap["datasetlifecycle"].(map[string]interface{})["isOnCentralDisk"] = false
	newMetaDataMap["datasetlifecycle"].(map[string]interface{})["archiveStatusMessage"] = "filesNotYetAvailable"
	newMetaDataMap["datasetlifecycle"].(map[string]interface{})["archivable"] = false

	datasetId, err := datasetIngestor.IngestDataset(http_client, SCIAT_API_URL, newMetaDataMap, fullFileArray, user)
	_ = datasetId
	if err != nil {
		log.Printf("")
		return "", err
	}
	switch task.TransferMethod {
	case TransferS3:
		_, err = UploadS3(task_context, app_context, datasetId, task.DatasetFolder.FolderPath, task.DatasetFolder.Id.String(), task.TransferOptions)
		// err := datasetIngestor.SyncLocalDataToFileserver(datasetId, user, RSYNCServer, datasetSourceFolder, absFileListing, os.Stdout)
	case TransferGlobus:
		// err := datasetIngestor.SyncLocalDataToFileserver(datasetId, user, RSYNCServer, datasetSourceFolder, absFileListing, os.Stdout)
	_:
		// err := datasetIngestor.SyncLocalDataToFileserver(datasetId, user, RSYNCServer, datasetSourceFolder, absFileListing, os.Stdout)
	}

	if err != nil {
		return datasetId, err
	}
	// upload data
	// mark dataset archivable
	err = datasetIngestor.MarkFilesReady(http_client, SCIAT_API_URL, datasetId, user)
	// if err != nil {
	// 	log.Fatal("Couldn't mark files ready:", err)
	// }
	return datasetId, err

	// mark dataset archivable
	// err := datasetIngestor.MarkFilesReady(client, APIServer, datasetId, user)
	// if err != nil {
	// 	log.Fatal("Couldn't mark files ready:", err)
	// }
}
