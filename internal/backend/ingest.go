package backend

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/paulscherrerinstitute/scicat-cli/v3/cmd/cliutils"
	"github.com/paulscherrerinstitute/scicat-cli/v3/datasetIngestor"
)

type DatasetBatch struct {
	MetaDataMap     map[string]interface{}
	SourceFolder    string
	BeamlineAccount bool
	AbsFileListing  string
}

type DatasetIngestRuntimeConfig struct {
	Tapecopies    int
	AddAttachment string
	AddCaption    string
	RSYNCServer   string
}

type IngestService struct {
	Base                 *TransportEngine
	FileSys              *FileService
	ArchivableDatasetIDs []string
}

func NewIngestService(base *TransportEngine, fileSys *FileService) *IngestService {
	return &IngestService{
		Base:    base,
		FileSys: fileSys,
	}
}

func (in *IngestService) PrepareBatch(metadataFile, absFileListing string) (*DatasetBatch, error) {
	metaMap, srcFolder, beamlineAcc, err := datasetIngestor.ReadAndCheckMetadata(
		in.Base.Client,
		in.Base.APIServer,
		metadataFile,
		in.Base.UserSession.User,
		in.Base.UserSession.AccessGroups,
	)
	if err != nil {
		return nil, fmt.Errorf("metadata validation failed for %s: %w", metadataFile, err)
	}

	return &DatasetBatch{
		MetaDataMap:     metaMap,
		SourceFolder:    srcFolder,
		BeamlineAccount: beamlineAcc,
		AbsFileListing:  absFileListing,
	}, nil
}

func (in *IngestService) CheckExistingSources(datasetPaths []string) (datasetIngestor.DatasetQuery, error) {
	foundList, err := datasetIngestor.TestForExistingSourceFolder(
		datasetPaths,
		in.Base.Client,
		in.Base.APIServer,
		in.Base.UserSession.User["accessToken"],
	)
	if err != nil {
		return nil, fmt.Errorf("failed to check existing source folders: %w", err)
	}
	return foundList, nil
}

func (in *IngestService) Ingest(
	batch *DatasetBatch,
	folder string,
	fileArray []datasetIngestor.Datafile,
	start time.Time,
	end time.Time,
	owner string,
	copyFlag bool,
	cfg DatasetIngestRuntimeConfig,
) (string, error) {

	in.PrepareMetadata(batch.MetaDataMap, folder, start, end, owner, cfg.Tapecopies)
	in.ApplyLifecycleProperties(batch.MetaDataMap, copyFlag)

	datasetId, err := in.RegisterDatasetRecord(batch.MetaDataMap, fileArray)
	if err != nil {
		return "", err
	}

	if cfg.AddAttachment != "" {
		in.UploadOptionalAttachment(datasetId, batch.MetaDataMap, cfg.AddAttachment, cfg.AddCaption)
	}

	archivable := !copyFlag
	if copyFlag {
		archivable = in.ExecuteDataTransfer(batch, datasetId, folder, fileArray, cfg)
	}

	if archivable {
		in.ArchivableDatasetIDs = append(in.ArchivableDatasetIDs, datasetId)
	}

	return datasetId, nil
}

func (in *IngestService) PrepareMetadata(metaDataMap map[string]interface{}, folder string, start time.Time, end time.Time, owner string, tapeCopies int) {
	var originalMap = make(map[string]string)
	datasetIngestor.UpdateMetaData(in.Base.Client, in.Base.APIServer, in.Base.UserSession.User, originalMap, metaDataMap, start, end, owner, tapeCopies)
	metaDataMap["sourceFolder"] = folder

	pretty, _ := json.MarshalIndent(metaDataMap, "", "    ")
	log.Printf("Updated metadata object:\n%s\n", pretty)
}

func (in *IngestService) ApplyLifecycleProperties(metaDataMap map[string]interface{}, copyFlag bool) {
	if _, ok := metaDataMap["datasetlifecycle"]; !ok {
		metaDataMap["datasetlifecycle"] = map[string]interface{}{}
	}
	lifecycle := metaDataMap["datasetlifecycle"].(map[string]interface{})

	lifecycle["isOnCentralDisk"] = !copyFlag
	if copyFlag {
		lifecycle["archiveStatusMessage"] = "filesNotYetAvailable"
	} else {
		lifecycle["archiveStatusMessage"] = "datasetCreated"
	}
	lifecycle["archivable"] = !copyFlag
}

func (in *IngestService) RegisterDatasetRecord(metaDataMap map[string]interface{}, fileArray []datasetIngestor.Datafile) (string, error) {
	log.Println("Ingesting dataset record to server...")
	datasetId, err := datasetIngestor.IngestDataset(in.Base.Client, in.Base.APIServer, metaDataMap, fileArray, in.Base.UserSession.User)
	if err != nil {
		return "", fmt.Errorf("failed to ingest dataset record: %w", err)
	}
	log.Println("Dataset created successfully with ID:", datasetId)
	return datasetId, nil
}

func (in *IngestService) UploadOptionalAttachment(datasetId string, metaDataMap map[string]interface{}, filePath, caption string) {
	err := datasetIngestor.AddAttachment(in.Base.Client, in.Base.APIServer, datasetId, metaDataMap, in.Base.UserSession.User["accessToken"], filePath, caption)
	if err != nil {
		log.Println("Warning: Couldn't upload attachment:", err)
		return
	}
	log.Printf("Attachment file %v successfully linked to dataset %v\n", filePath, datasetId)
}

func (in *IngestService) ExecuteDataTransfer(batch *DatasetBatch, datasetId string, folder string, fileArray []datasetIngestor.Datafile, cfg DatasetIngestRuntimeConfig) bool {
	var filePathList []string
	var isSymlinkList []bool
	for _, file := range fileArray {
		filePathList = append(filePathList, file.Path)
		isSymlinkList = append(isSymlinkList, file.IsSymlink)
	}

	params := cliutils.TransferParams{
		SshParams: cliutils.SshParams{
			Client: in.Base.Client, User: in.Base.UserSession.User, ApiServer: in.Base.APIServer, RsyncServer: cfg.RSYNCServer, AbsFilelistPath: batch.AbsFileListing,
		},
		GlobusParams: cliutils.GlobusParams{
			GlobusClient: in.FileSys.GlobusClient, SrcCollection: in.FileSys.GlobusConfig.SourceCollection, SrcPrefixPath: in.FileSys.GlobusConfig.SourcePrefixPath,
			DestCollection: in.FileSys.GlobusConfig.DestinationCollection, DestPrefixPath: in.FileSys.GlobusConfig.DestinationPrefixPath,
			Filelist: filePathList, IsSymlinkList: isSymlinkList,
		},
		DatasetId:           datasetId,
		DatasetSourceFolder: folder,
	}

	archivableState, err := in.FileSys.TransferFiles(params)
	if err != nil {
		log.Printf("Error during file transfer sequence: %v\n", err)
		return false
	}
	return archivableState
}
