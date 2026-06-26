package orchestrator

import (
	"bufio"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/SwissOpenEM/globus"
	"github.com/fatih/color"
	"github.com/paulscherrerinstitute/scicat-cli/v3/cmd/cliutils"
	"github.com/paulscherrerinstitute/scicat-cli/v3/datasetIngestor"
	"github.com/paulscherrerinstitute/scicat-cli/v3/datasetUtils"
	"github.com/spf13/cobra"
)

const DATASET_ID_PREFIX = "20.500.11935/"

type DatasetRegistrar func(client *http.Client, apiServer string, metaDataMap map[string]interface{}, fullFileArray []datasetIngestor.Datafile, user map[string]string) (string, error)

type AttachmentAdder func(client *http.Client, apiServer, datasetId string, metaDataMap map[string]interface{}, token, filename, caption string) error

type AvailabilityChecker func(username, rsyncServer, sourceFolder string, output io.Writer) (error, error)

type SourceFolderTester func(datasetPaths []string, client *http.Client, apiServer, token string) (datasetIngestor.DatasetQuery, error)

// *Changed fields record whether a flag was explicitly set, so runIngestionPipeline
// never needs to inspect cobra.Command after ParseConfig returns.
type IngestConfig struct {
	EnvConfig                        cliutils.InputEnvironmentConfig
	IngestFlag                       bool
	NoninteractiveFlag               bool
	Userpass                         string
	Token                            string
	Oidc                             bool
	CopyFlag                         bool
	CopyFlagChanged                  bool
	NocopyFlag                       bool
	NocopyFlagChanged                bool
	TransferTypeFlag                 string
	Tapecopies                       int
	AutoarchiveFlag                  bool
	Linkfiles                        string
	LinkfilesFlagChanged             bool
	AllowExistingSourceFolder        bool
	AllowExistingSourceFolderChanged bool
	AddAttachment                    string
	AddCaption                       string
	ShowVersion                      bool
	GlobusCfgFlag                    string
	GlobusCfgChanged                 bool
	RemoteFileScan                   bool
}

type DatasetArgs struct {
	MetadataFile       string
	DatasetFileListTxt string
	FolderListingTxt   string
	AbsFileListing     string
	DatasetStatus      datasetExistenceStatus
}

type ingestionResult struct {
	archivableDatasets []string
	ownerGroup         string
	user               map[string]string
	skippedLinks       uint
	illegalFileNames   uint
	emptyDatasets      int
	tooLargeDatasets   int
}

type datasetExistenceStatus struct {
	IsDatasetId         bool
	DatasetExists       bool
	OrigDatablocksExist bool
}

type FileContext struct {
	FullFileArray []datasetIngestor.Datafile
	StartTime     time.Time
	EndTime       time.Time
	Owner         string
	NumFiles      int64
	TotalSize     int64
}

// IngestionStrategy is the per-arg-type strategy. Two implementations are held
// in IngestContext: FileIngestion for metadata-file args and DatasetIdIngestion
// for dataset-ID args. runIngestionBeforeArchive selects one and calls it uniformly.
type IngestionStrategy interface {
	ReadMetadata(*http.Client, string, string, map[string]string, []string) (map[string]interface{}, string, bool, error)
	Ingest(*http.Client, string, map[string]interface{}, []datasetIngestor.Datafile, map[string]string) (string, error)
	// IngestRemote is used instead of Ingest when --remotefilescan is set.
	IngestRemote(*http.Client, string, map[string]interface{}, []datasetIngestor.Datafile, map[string]string) (string, error)
	AddAttachment(*http.Client, string, string, map[string]interface{}, string, string, string) error
}

// FileIngestion implements IngestionStrategy for metadata-file args.
type FileIngestion struct{}

func (FileIngestion) ReadMetadata(c *http.Client, api, arg string, user map[string]string, groups []string) (map[string]interface{}, string, bool, error) {
	return datasetIngestor.ReadAndCheckMetadata(c, api, arg, user, groups)
}
func (FileIngestion) Ingest(c *http.Client, api string, meta map[string]interface{}, files []datasetIngestor.Datafile, user map[string]string) (string, error) {
	return datasetIngestor.IngestDataset(c, api, meta, files, user)
}
func (FileIngestion) IngestRemote(c *http.Client, api string, meta map[string]interface{}, _ []datasetIngestor.Datafile, user map[string]string) (string, error) {
	return datasetIngestor.CreateDataset(c, api, meta, user)
}
func (FileIngestion) AddAttachment(c *http.Client, api, id string, meta map[string]interface{}, token, file, caption string) error {
	return datasetIngestor.AddAttachment(c, api, id, meta, token, file, caption)
}

// DatasetIdIngestion implements IngestionStrategy for dataset-ID args.
type DatasetIdIngestion struct{}

func (DatasetIdIngestion) ReadMetadata(c *http.Client, api, id string, user map[string]string, groups []string) (map[string]interface{}, string, bool, error) {
	return ReadAndCheckMetadataFromDatasetId(c, api, id, user, groups)
}
func (DatasetIdIngestion) Ingest(c *http.Client, api string, meta map[string]interface{}, files []datasetIngestor.Datafile, user map[string]string) (string, error) {
	id := meta["datasetId"].(string)
	return id, datasetIngestor.CreateOrigDatablocks(c, api, files, id, user)
}
func (d DatasetIdIngestion) IngestRemote(c *http.Client, api string, meta map[string]interface{}, files []datasetIngestor.Datafile, user map[string]string) (string, error) {
	return d.Ingest(c, api, meta, files, user)
}
func (DatasetIdIngestion) AddAttachment(*http.Client, string, string, map[string]interface{}, string, string, string) error {
	return nil
}

// IngestContext bundles all injectable functions so that runIngestionPipeline
// can be tested without touching any global or package-level state.
type IngestContext struct {
	Cfg           IngestConfig
	Client        *http.Client
	APIServer     string
	RsyncServer   string
	Scanner       *bufio.Scanner
	TransferFiles func(cliutils.TransferParams) (bool, error)
	GlobusClient  globus.GlobusClient
	GConfig       cliutils.GlobusConfig

	CheckForNewVersion          func(*http.Client, string, string)
	CheckForServiceAvailability func(*http.Client, bool, bool)
	Authenticate                func(cliutils.Authenticator, *http.Client, string, string, string, bool, ...func(...any)) (map[string]string, []string, error)
	TestForExistingSourceFolder SourceFolderTester
	CheckCentralAvailability    AvailabilityChecker
	UpdateMetaData              func(*http.Client, string, map[string]string, map[string]string, map[string]interface{}, time.Time, time.Time, string, int)
	ResetUpdatedMetaData        func(map[string]string, map[string]interface{})
	FileIngestion               IngestionStrategy
	DatasetIdIngestion          IngestionStrategy
	CreateArchivalJob           func(*http.Client, string, map[string]string, string, []string, *int, *time.Time) (string, error)
}

func ParseConfig(cmd *cobra.Command) IngestConfig {
	return IngestConfig{
		EnvConfig: cliutils.InputEnvironmentConfig{
			TestenvFlag:   cliutils.GetCobraBoolFlag(cmd, "testenv"),
			DevenvFlag:    cliutils.GetCobraBoolFlag(cmd, "devenv"),
			TunnelenvFlag: cliutils.GetCobraBoolFlag(cmd, "tunnelenv"),
			LocalenvFlag:  cliutils.GetCobraBoolFlag(cmd, "localenv"),
			ScicatUrl:     cliutils.GetCobraStringFlag(cmd, "scicat-url"),
			RsyncUrl:      cliutils.GetCobraStringFlag(cmd, "rsync-url"),
		},
		IngestFlag:                       cliutils.GetCobraBoolFlag(cmd, "ingest"),
		NoninteractiveFlag:               cliutils.GetCobraBoolFlag(cmd, "noninteractive"),
		Userpass:                         cliutils.GetCobraStringFlag(cmd, "user"),
		Token:                            cliutils.GetCobraStringFlag(cmd, "token"),
		Oidc:                             cliutils.GetCobraBoolFlag(cmd, "oidc"),
		CopyFlag:                         cliutils.GetCobraBoolFlag(cmd, "copy"),
		CopyFlagChanged:                  cmd.Flags().Lookup("copy").Changed,
		NocopyFlag:                       cliutils.GetCobraBoolFlag(cmd, "nocopy"),
		NocopyFlagChanged:                cmd.Flags().Lookup("nocopy").Changed,
		TransferTypeFlag:                 cliutils.GetCobraStringFlag(cmd, "transfer-type"),
		Tapecopies:                       cliutils.GetCobraIntFlag(cmd, "tapecopies"),
		AutoarchiveFlag:                  cliutils.GetCobraBoolFlag(cmd, "autoarchive"),
		Linkfiles:                        cliutils.GetCobraStringFlag(cmd, "linkfiles"),
		LinkfilesFlagChanged:             cmd.Flags().Lookup("linkfiles").Changed,
		AllowExistingSourceFolder:        cliutils.GetCobraBoolFlag(cmd, "allowexistingsource"),
		AllowExistingSourceFolderChanged: cmd.Flags().Lookup("allowexistingsource").Changed,
		AddAttachment:                    cliutils.GetCobraStringFlag(cmd, "addattachment"),
		AddCaption:                       cliutils.GetCobraStringFlag(cmd, "addcaption"),
		ShowVersion:                      cliutils.GetCobraBoolFlag(cmd, "version"),
		GlobusCfgFlag:                    cliutils.GetCobraStringFlag(cmd, "globus-cfg"),
		GlobusCfgChanged:                 cmd.Flags().Lookup("globus-cfg").Changed,
		RemoteFileScan:                   cliutils.GetCobraBoolFlag(cmd, "remotefilescan"),
	}
}

func isDatasetId(s string) bool {
	return strings.HasPrefix(strings.ToLower(s), DATASET_ID_PREFIX)
}

func ParseAndValidateArgs(args []string) (DatasetArgs, error) {
	dArgs := DatasetArgs{
		MetadataFile: args[0],
	}
	if len(args) == 2 {
		if filepath.Base(args[1]) == "folderlisting.txt" {
			dArgs.FolderListingTxt = args[1]
		} else {
			abs, err := filepath.Abs(args[1])
			if err != nil {
				return DatasetArgs{}, fmt.Errorf("cannot resolve absolute path for %q: %w", args[1], err)
			}
			dArgs.DatasetFileListTxt = args[1]
			dArgs.AbsFileListing = abs
		}
	}
	return dArgs, nil
}

func ParseAndValidateSeparatorArg(arg string) (DatasetArgs, error) {
	meta, fileList, _ := strings.Cut(arg, "@")
	if meta == "" || filepath.Ext(meta) != ".json" {
		return DatasetArgs{}, fmt.Errorf("invalid argument %q: metadata file cannot be empty or must have a .json extension", arg)
	}
	if fileList != "" && filepath.Ext(fileList) != ".txt" {
		return DatasetArgs{}, fmt.Errorf("invalid argument %q: file list must have a .txt extension", arg)
	}
	if fileList == "" {
		return ParseAndValidateArgs([]string{meta})
	}
	return ParseAndValidateArgs([]string{meta, fileList})
}

func ParseAndValidateAllArgs(args []string) ([]DatasetArgs, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("no execution arguments provided")
	}

	for _, arg := range args {
		if err := ValidateArgumentFormat(arg); err != nil {
			return nil, err
		}
	}

	// Legacy mode: no colons anywhere — delegate to the single/double positional arg parser.
	if isLegacyMode(args) {
		singleArg, err := ParseAndValidateArgs(args)
		if err != nil {
			return nil, fmt.Errorf("invalid arguments: %w", err)
		}
		return []DatasetArgs{singleArg}, nil
	}

	// Colon mode: every arg is "meta.json[:filelist.txt]".
	allArgs := make([]DatasetArgs, 0, len(args))
	for i, arg := range args {
		if isDatasetId(arg) {
			allArgs = append(allArgs, DatasetArgs{
				MetadataFile:  arg,
				DatasetStatus: datasetExistenceStatus{IsDatasetId: true},
			})
			continue
		}
		dArgs, err := ParseAndValidateSeparatorArg(arg)
		if err != nil {
			return nil, fmt.Errorf("invalid argument at position %d: %w", i+1, err)
		}
		allArgs = append(allArgs, dArgs)
	}
	return allArgs, nil
}

func isLegacyMode(args []string) bool {
	return len(args) == 2 &&
		filepath.Ext(args[1]) == ".txt" &&
		!strings.Contains(args[0], "@") &&
		!strings.Contains(args[1], "@")
}

func ValidateArgumentFormat(arg string) error {
	cleanArg := strings.TrimSpace(arg)
	if cleanArg == "" {
		return fmt.Errorf("argument cannot be empty")
	}

	if isDatasetId(cleanArg) {
		return nil
	}

	if left, right, found := strings.Cut(cleanArg, "@"); found {
		if strings.HasSuffix(cleanArg, "@") {
			return fmt.Errorf("invalid argument format %q: argument cannot end with @", arg)
		}
		if filepath.Ext(left) != ".json" {
			return fmt.Errorf("invalid format in pair %q: left side must have a .json extension", arg)
		}
		if right != "" && filepath.Ext(right) != ".txt" {
			return fmt.Errorf("invalid format in pair %q: right side must have a .txt extension", arg)
		}
		return nil
	}

	ext := filepath.Ext(cleanArg)
	if ext == ".json" || ext == ".txt" {
		return nil
	}

	return fmt.Errorf("invalid argument format %q: must be 'metadata.json@filelist.txt', '.json', or '.txt'", arg)
}

func SetupTransferStrategy(cfg IngestConfig) (func(cliutils.TransferParams) (bool, error), globus.GlobusClient, cliutils.GlobusConfig, error) {
	transferType, err := cliutils.ConvertToTransferType(cfg.TransferTypeFlag)
	if err != nil {
		return nil, globus.GlobusClient{}, cliutils.GlobusConfig{}, err
	}

	var (
		transferFiles func(cliutils.TransferParams) (bool, error)
		globusClient  globus.GlobusClient
		gConfig       cliutils.GlobusConfig
	)

	switch transferType {
	case cliutils.Ssh:
		transferFiles = cliutils.SshTransfer
	case cliutils.Globus:
		transferFiles = cliutils.GlobusTransfer
		var globusConfigPath string
		if cfg.GlobusCfgChanged {
			globusConfigPath = cfg.GlobusCfgFlag
		} else {
			execPath, err := os.Executable()
			if err != nil {
				return nil, globus.GlobusClient{}, cliutils.GlobusConfig{}, fmt.Errorf("can't find executable path: %w", err)
			}
			globusConfigPath = filepath.Join(filepath.Dir(execPath), "globus.yaml")
		}
		globusClient, gConfig, err = cliutils.GlobusLogin(globusConfigPath)
		if err != nil {
			return nil, globus.GlobusClient{}, cliutils.GlobusConfig{}, fmt.Errorf("couldn't create globus client: %w", err)
		}
		if cfg.AutoarchiveFlag {
			return nil, globus.GlobusClient{}, cliutils.GlobusConfig{}, errors.New("cannot autoarchive with Globus; use the \"globusCheckTransfer\" command instead")
		}
	}

	return transferFiles, globusClient, gConfig, nil
}

func ResolveDatasetPaths(metadataSourceFolder, folderListingTxt string) ([]string, error) {
	if folderListingTxt == "" {
		return []string{metadataSourceFolder}, nil
	}

	data, err := os.ReadFile(folderListingTxt)
	if err != nil {
		return nil, err
	}

	var paths []string
	for _, line := range strings.Split(string(data), "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.Split(line, "/")
		if len(parts) > 3 && parts[3] == "data" {
			real, err := filepath.EvalSymlinks(line)
			if err != nil {
				return nil, fmt.Errorf("failed to find canonical form of sourceFolder %v: %w", line, err)
			}
			color.Set(color.FgYellow)
			log.Printf("Transform sourceFolder %v to canonical form: %v\n", line, real)
			color.Unset()
			paths = append(paths, real)
		} else {
			paths = append(paths, line)
		}
	}
	return paths, nil
}

func GuardExistingSourceFolders(
	scanner *bufio.Scanner,
	datasetPaths []string,
	client *http.Client,
	apiServer, token string,
	allowExisting, flagChanged bool,
	testFn SourceFolderTester,
) error {
	log.Println("Testing for existing source folders...")
	foundList, err := testFn(datasetPaths, client, apiServer, token)
	if err != nil {
		return err
	}

	if len(foundList) == 0 {
		log.Println("Finished testing for existing source folders.")
		return nil
	}

	color.Set(color.FgYellow)
	fmt.Println("Warning! The following datasets have been found with the same sourceFolders: ")
	for _, element := range foundList {
		fmt.Printf("  - PID: \"%s\", sourceFolder: \"%s\"\n", element.Pid, element.SourceFolder)
	}
	color.Unset()

	if allowExisting {
		return nil
	}
	if flagChanged {
		return errors.New("existing source folders are not allowed")
	}
	log.Printf("Do you want to ingest the corresponding new datasets nevertheless (y/N) ? ")
	scanner.Scan()
	if scanner.Text() != "y" {
		return errors.New("aborted")
	}
	return nil
}

func GatherFiles(datasetSourceFolder, datasetFileListTxt string, skipSymlinks *string, skippedLinks, illegalFileNames *uint) (FileContext, error) {
	if !(*skipSymlinks == "sA" || *skipSymlinks == "kA" || *skipSymlinks == "dA") {
		*skipSymlinks = ""
	}

	localSymlinkCallback := CreateLocalSymlinkCallbackForFileLister(skipSymlinks, skippedLinks)
	localFilepathFilterCallback := CreateLocalFilenameFilterCallback(illegalFileNames)

	log.Printf("Scanning files in dataset %s", datasetSourceFolder)
	log.Printf("Getting filelist for \"%s\"...\n", datasetSourceFolder)
	fullFileArray, startTime, endTime, owner, numFiles, totalSize, err :=
		datasetIngestor.GetLocalFileList(datasetSourceFolder, datasetFileListTxt, localSymlinkCallback, localFilepathFilterCallback)
	if err == nil {
		log.Println("File list collected.")
		log.Printf("The dataset contains %v files with a total size of %v bytes.\n", numFiles, totalSize)
	}

	return FileContext{
		FullFileArray: fullFileArray,
		StartTime:     startTime,
		EndTime:       endTime,
		Owner:         owner,
		NumFiles:      numFiles,
		TotalSize:     totalSize,
	}, err
}

// VerifyCentralAvailability returns (true, nil) when a copy is required,
// (false, nil) when the data is already centrally available, and (false, err)
// on any error or user abort.
func VerifyCentralAvailability(
	cfg IngestConfig,
	rsyncServer, datasetSourceFolder string,
	user map[string]string,
	accessGroups []string,
	scanner *bufio.Scanner,
	checkFn AvailabilityChecker,
) (bool, error) {
	log.Println("Checking if data is centrally available...")
	sshErr, otherErr := checkFn(user["username"], rsyncServer, datasetSourceFolder, os.Stdout)
	if otherErr != nil {
		return false, fmt.Errorf("cannot check if data is centrally available: %w", otherErr)
	}

	if sshErr == nil {
		log.Println("Data is present centrally.")
		return false, nil
	}

	color.Set(color.FgYellow)
	log.Printf("The source folder %v is not centrally available.\nThe data must first be copied.\n ", datasetSourceFolder)
	color.Unset()

	if len(accessGroups) == 0 {
		return false, errors.New("for copying, you must use a personal account; beamline accounts are not supported")
	}
	if !cfg.NoninteractiveFlag {
		log.Printf("Do you want to continue (Y/n)? ")
		scanner.Scan()
		if scanner.Text() == "n" {
			return false, errors.New("further ingests interrupted because copying is needed, but no copy wanted")
		}
	}
	return true, nil
}

func InitializeLifecycleFields(metaDataMap map[string]interface{}, requiresCopy bool) bool {
	if _, ok := metaDataMap["datasetlifecycle"]; !ok {
		metaDataMap["datasetlifecycle"] = map[string]interface{}{}
	}
	lifecycle := metaDataMap["datasetlifecycle"].(map[string]interface{})
	if requiresCopy {
		lifecycle["isOnCentralDisk"] = false
		lifecycle["archiveStatusMessage"] = "filesNotYetAvailable"
		lifecycle["archivable"] = false
		return false
	}
	lifecycle["isOnCentralDisk"] = true
	lifecycle["archiveStatusMessage"] = "datasetCreated"
	lifecycle["archivable"] = true
	return true
}

func RegisterDatasetWithCatalog(
	client *http.Client,
	apiServer string,
	metaDataMap map[string]interface{},
	fileCtx FileContext,
	user map[string]string,
	cfg IngestConfig,
	ingestFn DatasetRegistrar,
	attachFn AttachmentAdder,
) (string, error) {
	log.Println("Ingesting dataset...")
	datasetId, err := ingestFn(client, apiServer, metaDataMap, fileCtx.FullFileArray, user)
	if err != nil {
		return "", fmt.Errorf("couldn't ingest dataset: %w", err)
	}
	log.Println("Dataset created:", datasetId)

	if cfg.AddAttachment != "" {
		log.Println("Adding attachment...")
		if err := attachFn(client, apiServer, datasetId, metaDataMap, user["accessToken"], cfg.AddAttachment, cfg.AddCaption); err != nil {
			log.Println("Couldn't add attachment:", err)
		} else {
			log.Printf("Attachment file %v added to dataset %v\n", cfg.AddAttachment, datasetId)
		}
	}
	return datasetId, nil
}

func ExecuteFileTransfer(
	client *http.Client,
	apiServer, rsyncServer, datasetId, datasetSourceFolder, absFileListing string,
	user map[string]string,
	fileCtx FileContext,
	transferFiles func(cliutils.TransferParams) (bool, error),
	globusClient globus.GlobusClient,
	gConfig cliutils.GlobusConfig,
	transferTypeFlag string,
	markFilesReady bool,
) bool {
	filePathList := make([]string, 0, len(fileCtx.FullFileArray))
	isSymlinkList := make([]bool, 0, len(fileCtx.FullFileArray))

	for _, file := range fileCtx.FullFileArray {
		filePathList = append(filePathList, file.Path)
		isSymlinkList = append(isSymlinkList, file.IsSymlink)
	}

	params := cliutils.TransferParams{
		SshParams: cliutils.SshParams{
			Client:          client,
			User:            user,
			ApiServer:       apiServer,
			RsyncServer:     rsyncServer,
			AbsFilelistPath: absFileListing,
			MarkFilesReady:  markFilesReady,
		},
		GlobusParams: cliutils.GlobusParams{
			GlobusClient:   globusClient,
			SrcCollection:  gConfig.SourceCollection,
			SrcPrefixPath:  gConfig.SourcePrefixPath,
			DestCollection: gConfig.DestinationCollection,
			DestPrefixPath: gConfig.DestinationPrefixPath,
			Filelist:       filePathList,
			IsSymlinkList:  isSymlinkList,
		},
		DatasetId:           datasetId,
		DatasetSourceFolder: datasetSourceFolder,
	}

	archivable, transferErr := transferFiles(params)
	if transferErr != nil {
		color.Set(color.FgRed)
		log.Printf("The command to copy files exited with error %v \n", transferErr)
		log.Printf("The dataset %v is not yet in an archivable state\n", datasetId)
		color.Unset()
	}
	if transferErr == nil && !archivable {
		color.Set(color.FgYellow)
		log.Println("The command finished successfully, however the dataset is not yet archivable.")
		log.Println("This means that the dataset has to be marked as archivable after the asynchronous transfer has finished.")
		log.Printf("Please consult the %s transfer type's doc for handling this.\n", transferTypeFlag)
		color.Unset()
	}
	return archivable
}

func submitAndPrintResults(ictx IngestContext, user map[string]string, ownerGroup string, archivableDatasets []string) error {
	for _, id := range archivableDatasets {
		fmt.Println(id)
	}
	if !ictx.Cfg.AutoarchiveFlag || !ictx.Cfg.IngestFlag || len(archivableDatasets) == 0 {
		return nil
	}
	log.Printf("Submitting Archive Job for the ingested datasets.\n")
	jobId, err := ictx.CreateArchivalJob(ictx.Client, ictx.APIServer, user, ownerGroup, archivableDatasets, &ictx.Cfg.Tapecopies, nil)
	if err != nil {
		color.Set(color.FgRed)
		log.Printf("Could not create the archival job: %s\n", err.Error())
		color.Unset()
		return fmt.Errorf("could not create the archival job: %w", err)
	}
	log.Println("Submitted job:", jobId)
	return nil
}

// ReadAndCheckMetadataFromDatasetId satisfies the ReadAndCheckMetadata contract
// for existing dataset IDs. The bool return is true when orig datablocks already
// exist (ds.Size > 0), false when they still need to be created.
func ReadAndCheckMetadataFromDatasetId(client *http.Client, apiServer, datasetId string, user map[string]string, _ []string) (map[string]interface{}, string, bool, error) {
	metadataArray, missing, err := datasetUtils.GetDatasetDetails(client, apiServer, user["accessToken"], []string{datasetId}, "")
	if err != nil {
		return nil, "", false, fmt.Errorf("failed to fetch dataset %q: %w", datasetId, err)
	}
	if len(missing) > 0 {
		return nil, "", false, fmt.Errorf("dataset %q not found", datasetId)
	}

	ds := metadataArray[0]
	metaDataMap := map[string]interface{}{
		"datasetId":    ds.Pid,
		"sourceFolder": ds.SourceFolder,
		"ownerGroup":   ds.OwnerGroup,
	}

	return metaDataMap, ds.SourceFolder, ds.Size != 0, nil
}

// RunIngestionPipeline is the Cobra entry point. It wires real implementations
// into IngestContext and is the only function in this package that calls log.Fatal.
func RunIngestionPipeline(cmd *cobra.Command, args []string, version string) {
	cfg := ParseConfig(cmd)
	client := &http.Client{
		Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: false}},
		Timeout:   120 * time.Second,
	}
	transferFiles, globusClient, gConfig, err := SetupTransferStrategy(cfg)
	if err != nil {
		log.Fatal(err)
	}

	if cfg.RemoteFileScan {
		cfg.NocopyFlag = true
		cfg.NocopyFlagChanged = true
	}

	ctx := IngestContext{
		Cfg:           cfg,
		Client:        client,
		APIServer:     cfg.EnvConfig.ResolveAPIServer(),
		RsyncServer:   cfg.EnvConfig.ResolveRSYNCServer(),
		TransferFiles: transferFiles,
		GlobusClient:  globusClient,
		GConfig:       gConfig,
		Scanner:       bufio.NewScanner(os.Stdin),

		CheckForNewVersion:          datasetUtils.CheckForNewVersion,
		CheckForServiceAvailability: datasetUtils.CheckForServiceAvailability,
		Authenticate:                cliutils.Authenticate,
		TestForExistingSourceFolder: datasetIngestor.TestForExistingSourceFolder,
		CheckCentralAvailability:    datasetIngestor.CheckDataCentrallyAvailableSsh,
		UpdateMetaData:              datasetIngestor.UpdateMetaData,
		ResetUpdatedMetaData:        datasetIngestor.ResetUpdatedMetaData,
		FileIngestion:               FileIngestion{},
		DatasetIdIngestion:          DatasetIdIngestion{},
		CreateArchivalJob:           datasetUtils.CreateArchivalJob,
	}

	dArgs, err := ParseAndValidateAllArgs(args)
	if err != nil {
		log.Fatal(err)
	}

	if len(dArgs) > 1 && cfg.AutoarchiveFlag {
		color.Set(color.FgYellow)
		fmt.Println("All datasets must share the same 'ownerGroup' or auto-archive will fail.")
		color.Unset()
	}

	if err := runIngestionPipeline(ctx, dArgs, version); err != nil {
		log.Fatal(err)
	}
}

func runParallelIngestionPipeline(ictx IngestContext, dArgs []DatasetArgs, version string, runResult *ingestionResult) error {
	type singleRunResult struct {
		res ingestionResult
		err error
	}

	resultCh := make(chan singleRunResult, len(dArgs))
	var wg sync.WaitGroup
	for _, dArg := range dArgs {
		wg.Add(1)
		go func(d DatasetArgs) {
			defer wg.Done()
			res, err := runIngestionBeforeArchive(ictx, d, version)
			resultCh <- singleRunResult{res: res, err: err}
		}(dArg)
	}
	wg.Wait()
	close(resultCh)

	var failed bool
	for r := range resultCh {
		if r.err != nil {
			log.Println(r.err)
			failed = true
			continue
		}
		updateResultCounters(runResult, r.res)
	}
	if failed {
		return errors.New("one or more ingestions failed")
	}
	return nil
}

func runIngestionPipeline(ictx IngestContext, dArgs []DatasetArgs, version string) error {
	runResult := ingestionResult{
		archivableDatasets: []string{},
		user:               nil,
		ownerGroup:         "",
		skippedLinks:       0,
		illegalFileNames:   0,
		emptyDatasets:      0,
		tooLargeDatasets:   0,
	}

	if !ictx.Cfg.NoninteractiveFlag && len(dArgs) > 1 {
		log.Println("Note: running ingestions sequentially because --noninteractive is not set. Pass --noninteractive to run them in parallel.")
	}

	if ictx.Cfg.NoninteractiveFlag && len(dArgs) > 1 {
		err := runParallelIngestionPipeline(ictx, dArgs, version, &runResult)
		if err != nil {
			return err
		}
	} else {
		for _, dArg := range dArgs {
			res, err := runIngestionBeforeArchive(ictx, dArg, version)
			if err != nil {
				return fmt.Errorf("error ingesting dataset with metadata %q: %w", dArg.MetadataFile, err)
			}
			updateResultCounters(&runResult, res)
		}
	}

	if !ictx.Cfg.IngestFlag {
		color.Set(color.FgRed)
		log.Printf("Note: you run in 'dry' mode to simply to check data consistency. Use the --ingest flag to really ingest datasets.")
		color.Unset()
	}

	if runResult.skippedLinks > 0 {
		color.Set(color.FgYellow)
		log.Printf("Total number of link files skipped: %v\n", runResult.skippedLinks)
		color.Unset()
	}
	if runResult.illegalFileNames > 0 {
		color.Set(color.FgRed)
		log.Printf("Number of files ignored because of illegal filenames: %v\n", runResult.illegalFileNames)
		color.Unset()
	}

	if runResult.emptyDatasets > 0 || runResult.tooLargeDatasets > 0 {
		return fmt.Errorf("errors encountered with dataset layouts: %d empty, %d too large", runResult.emptyDatasets, runResult.tooLargeDatasets)
	}

	return submitAndPrintResults(ictx, runResult.user, runResult.ownerGroup, runResult.archivableDatasets)
}

func updateResultCounters(result *ingestionResult, res ingestionResult) {
	result.archivableDatasets = append(result.archivableDatasets, res.archivableDatasets...)
	result.skippedLinks += res.skippedLinks
	result.illegalFileNames += res.illegalFileNames
	result.emptyDatasets += res.emptyDatasets
	result.tooLargeDatasets += res.tooLargeDatasets
	if result.user == nil {
		result.user = res.user
	}
	if result.ownerGroup == "" {
		result.ownerGroup = res.ownerGroup
	}
}

// runIngestionBeforeArchive is the per-dataset core. It operates entirely
// through ictx and never reads package-level state except for the project-wide
// datasetUtils.TestFlags/TestArgs hooks.
func runIngestionBeforeArchive(ictx IngestContext, dArgs DatasetArgs, version string) (ingestionResult, error) {
	tooLargeDatasets := 0
	emptyDatasets := 0
	originalMap := make(map[string]string)

	if datasetUtils.TestFlags != nil {
		datasetUtils.TestFlags(map[string]interface{}{
			"ingest":              ictx.Cfg.IngestFlag,
			"testenv":             ictx.Cfg.EnvConfig.TestenvFlag,
			"devenv":              ictx.Cfg.EnvConfig.DevenvFlag,
			"localenv":            ictx.Cfg.EnvConfig.LocalenvFlag,
			"tunnelenv":           ictx.Cfg.EnvConfig.TunnelenvFlag,
			"scicat-url":          ictx.Cfg.EnvConfig.ScicatUrl,
			"rsync-url":           ictx.Cfg.EnvConfig.RsyncUrl,
			"noninteractive":      ictx.Cfg.NoninteractiveFlag,
			"user":                ictx.Cfg.Userpass,
			"token":               ictx.Cfg.Token,
			"copy":                ictx.Cfg.CopyFlag,
			"nocopy":              ictx.Cfg.NocopyFlag,
			"tapecopies":          ictx.Cfg.Tapecopies,
			"autoarchive":         ictx.Cfg.AutoarchiveFlag,
			"linkfiles":           ictx.Cfg.Linkfiles,
			"allowexistingsource": ictx.Cfg.AllowExistingSourceFolder,
			"addattachment":       ictx.Cfg.AddAttachment,
			"addcaption":          ictx.Cfg.AddCaption,
			"version":             ictx.Cfg.ShowVersion,
			"remotefilescan":      ictx.Cfg.RemoteFileScan,
		})
		return ingestionResult{}, nil
	}

	if datasetUtils.TestArgs != nil {
		datasetUtils.TestArgs([]interface{}{dArgs.MetadataFile, dArgs.DatasetFileListTxt, dArgs.FolderListingTxt})
		return ingestionResult{}, nil
	}

	if ictx.Cfg.ShowVersion {
		fmt.Printf("%s\n", version)
		return ingestionResult{}, nil
	}

	ictx.CheckForNewVersion(ictx.Client, "datasetIngestor", version)
	ictx.CheckForServiceAvailability(ictx.Client, ictx.Cfg.EnvConfig.TestenvFlag, ictx.Cfg.AutoarchiveFlag)

	user, accessGroups, err := ictx.Authenticate(cliutils.RealAuthenticator{}, ictx.Client, ictx.APIServer, ictx.Cfg.Userpass, ictx.Cfg.Token, ictx.Cfg.Oidc)
	if err != nil {
		return ingestionResult{}, err
	}

	strategy := ictx.FileIngestion
	if dArgs.DatasetStatus.IsDatasetId {
		strategy = ictx.DatasetIdIngestion
	}
	metaDataMap, metadataSourceFolder, extraBool, err := strategy.ReadMetadata(ictx.Client, ictx.APIServer, dArgs.MetadataFile, user, accessGroups)
	if err != nil {
		return ingestionResult{}, fmt.Errorf("error in CheckMetadata function: %w", err)
	}

	// extraBool means beamlineAccount for metadata-file args and origDatablocksExist
	// for dataset-ID args; interpret it based on which mode we are in.
	datasetStatus := dArgs.DatasetStatus
	var beamlineAccount bool
	if datasetStatus.IsDatasetId {
		datasetStatus.DatasetExists = true
		datasetStatus.OrigDatablocksExist = extraBool
	} else {
		beamlineAccount = extraBool
	}

	archivableDatasetListOwnerGroup, ok := metaDataMap["ownerGroup"].(string)
	if !ok {
		return ingestionResult{}, errors.New("can't recover ownerGroup")
	}
	if datasetStatus.OrigDatablocksExist {
		// Dataset and orig datablocks already exist — nothing to ingest, just archive.
		return ingestionResult{
			archivableDatasets: []string{metaDataMap["datasetId"].(string)},
			ownerGroup:         archivableDatasetListOwnerGroup,
			user:               user,
		}, nil
	}

	datasetPaths, err := ResolveDatasetPaths(metadataSourceFolder, dArgs.FolderListingTxt)
	if err != nil {
		return ingestionResult{}, err
	}

	if !datasetStatus.DatasetExists {
		if err := GuardExistingSourceFolders(
			ictx.Scanner, datasetPaths, ictx.Client, ictx.APIServer, user["accessToken"],
			ictx.Cfg.AllowExistingSourceFolder, ictx.Cfg.AllowExistingSourceFolderChanged,
			ictx.TestForExistingSourceFolder,
		); err != nil {
			return ingestionResult{}, err
		}
	}

	if ictx.Cfg.NocopyFlag {
		ictx.Cfg.CopyFlag = false
	}
	checkCentralAvailability := !(ictx.Cfg.CopyFlagChanged || ictx.Cfg.NocopyFlagChanged || beamlineAccount || ictx.Cfg.CopyFlag)

	skipSymlinks := ""
	if ictx.Cfg.LinkfilesFlagChanged {
		switch ictx.Cfg.Linkfiles {
		case "delete":
			skipSymlinks = "sA"
		case "keep":
			skipSymlinks = "kA"
		default:
			skipSymlinks = "dA"
		}
	}

	var skippedLinks, illegalFileNames uint
	var archivableDatasetList []string

	for _, datasetSourceFolder := range datasetPaths {
		if datasetSourceFolder == "" {
			continue
		}

		log.Printf("===== Ingesting: \"%s\" =====\n", datasetSourceFolder)
		metaDataMap["sourceFolder"] = datasetSourceFolder

		var err error
		var fileCtx FileContext
		if !ictx.Cfg.RemoteFileScan {
			fileCtx, err = GatherFiles(datasetSourceFolder, dArgs.DatasetFileListTxt, &skipSymlinks, &skippedLinks, &illegalFileNames)
			if err != nil {
				return ingestionResult{}, fmt.Errorf("can't gather filelist of %q: %w", datasetSourceFolder, err)
			}

			if fileCtx.TotalSize == 0 {
				emptyDatasets++
				color.Set(color.FgRed)
				log.Printf("\"%s\" dataset cannot be ingested - contains no files\n", datasetSourceFolder)
				color.Unset()
				continue
			}
			if fileCtx.NumFiles > cliutils.TOTAL_MAXFILES {
				tooLargeDatasets++
				color.Set(color.FgRed)
				log.Printf("\"%s\" dataset cannot be ingested - too many files: has %d, max. %d\n", datasetSourceFolder, fileCtx.NumFiles, cliutils.TOTAL_MAXFILES)
				color.Unset()
				continue
			}

			if !datasetStatus.DatasetExists {
				ictx.UpdateMetaData(ictx.Client, ictx.APIServer, user, originalMap, metaDataMap, fileCtx.StartTime, fileCtx.EndTime, fileCtx.Owner, ictx.Cfg.Tapecopies)
				if pretty, err := json.MarshalIndent(metaDataMap, "", "    "); err != nil {
					log.Printf("Warning: could not marshal metadata for display: %v\n", err)
				} else {
					log.Printf("Updated metadata object:\n%s\n", pretty)
				}
			}
		}

		if ictx.Cfg.Tapecopies == 2 {
			color.Set(color.FgYellow)
			log.Println("Note: this dataset, if archived, will be copied to two tape copies")
			color.Unset()
		}

		requiresCopy := ictx.Cfg.CopyFlag
		if !datasetStatus.DatasetExists && checkCentralAvailability {
			requiresCopy, err = VerifyCentralAvailability(ictx.Cfg, ictx.RsyncServer, datasetSourceFolder, user, accessGroups, ictx.Scanner, ictx.CheckCentralAvailability)
			if err != nil {
				return ingestionResult{}, err
			}
		}

		if !ictx.Cfg.IngestFlag {
			ictx.ResetUpdatedMetaData(originalMap, metaDataMap)
			continue
		}

		archivable := InitializeLifecycleFields(metaDataMap, requiresCopy)
		if ictx.Cfg.RemoteFileScan {
			if lifecycle, ok := metaDataMap["datasetlifecycle"].(map[string]interface{}); ok {
				lifecycle["archiveStatusMessage"] = "origDatablocksNotYetAvailable"
			}
		}
		ingestFn := strategy.Ingest
		if ictx.Cfg.RemoteFileScan {
			ingestFn = strategy.IngestRemote
		}
		datasetId, err := RegisterDatasetWithCatalog(ictx.Client, ictx.APIServer, metaDataMap, fileCtx, user, ictx.Cfg, ingestFn, strategy.AddAttachment)
		if err != nil {
			return ingestionResult{}, err
		}

		if requiresCopy {
			archivable = ExecuteFileTransfer(ictx.Client, ictx.APIServer, ictx.RsyncServer, datasetId, datasetSourceFolder, dArgs.AbsFileListing, user, fileCtx, ictx.TransferFiles, ictx.GlobusClient, ictx.GConfig, ictx.Cfg.TransferTypeFlag, !ictx.Cfg.RemoteFileScan)
		}

		if archivable {
			archivableDatasetList = append(archivableDatasetList, datasetId)
		}

		ictx.ResetUpdatedMetaData(originalMap, metaDataMap)
	}

	return ingestionResult{
		archivableDatasets: archivableDatasetList,
		ownerGroup:         archivableDatasetListOwnerGroup,
		user:               user,
		skippedLinks:       skippedLinks,
		illegalFileNames:   illegalFileNames,
		emptyDatasets:      emptyDatasets,
		tooLargeDatasets:   tooLargeDatasets,
	}, nil
}

func CreateLocalSymlinkCallbackForFileLister(skipSymlinks *string, skippedLinks *uint) func(symlinkPath string, sourceFolder string) (bool, error) {
	scanner := bufio.NewScanner(os.Stdin)
	return func(symlinkPath string, sourceFolder string) (bool, error) {
		keep := true
		pointee, err := os.Readlink(symlinkPath)
		if err != nil {
			return false, fmt.Errorf("could not read symlink %v: %w", symlinkPath, err)
		}
		if !filepath.IsAbs(pointee) {
			symlinkAbs, err := filepath.Abs(filepath.Dir(symlinkPath))
			if err != nil {
				return false, err
			}
			pointeeAbs := filepath.Join(symlinkAbs, pointee)
			pointee, err = filepath.EvalSymlinks(pointeeAbs)
			if err != nil {
				log.Printf("Could not follow symlink for file:%v %v", pointeeAbs, err)
				keep = false
			}
		}
		switch {
		case *skipSymlinks == "ka" || *skipSymlinks == "kA":
			keep = true
		case *skipSymlinks == "sa" || *skipSymlinks == "sA":
			keep = false
		case *skipSymlinks == "da" || *skipSymlinks == "dA":
			keep = strings.HasPrefix(pointee, sourceFolder)
		default:
			color.Set(color.FgYellow)
			log.Printf("Warning: the file %s is a link pointing to %v.", symlinkPath, pointee)
			color.Unset()
			log.Printf(`
Please test if this link is meaningful and not pointing
outside the sourceFolder %s. The default behaviour is to
keep only internal links within a source folder.
You can also specify that you want to apply the same answer to ALL
subsequent links within the current dataset, by appending an a (dA,ka,sa).
If you want to give the same answer even to all subsequent datasets
in this command then specify a capital 'A', e.g. (dA,kA,sA)
Do you want to keep the link in dataset or skip it (D(efault)/k(eep)/s(kip) ?`, sourceFolder)
			scanner.Scan()
			*skipSymlinks = scanner.Text()
			if *skipSymlinks == "" {
				*skipSymlinks = "d"
			}
			if *skipSymlinks == "d" || *skipSymlinks == "dA" {
				keep = strings.HasPrefix(pointee, sourceFolder)
			} else {
				keep = (*skipSymlinks != "s" && *skipSymlinks != "sa" && *skipSymlinks != "sA")
			}
		}
		if keep {
			color.Set(color.FgGreen)
			log.Printf("You chose to keep the link %v -> %v.\n\n", symlinkPath, pointee)
		} else {
			color.Set(color.FgRed)
			*skippedLinks++
			log.Printf("You chose to remove the link %v -> %v.\n\n", symlinkPath, pointee)
		}
		color.Unset()
		return keep, nil
	}
}

func CreateLocalFilenameFilterCallback(illegalFileNamesCounter *uint) func(string) bool {
	return func(fp string) bool {
		if strings.ContainsAny(fp, "*\\") {
			color.Set(color.FgRed)
			log.Printf("Warning: the file %s contains illegal characters like *,\\ and will not be archived.", fp)
			color.Unset()
			if illegalFileNamesCounter != nil {
				*illegalFileNamesCounter++
			}
			return false
		}
		if strings.Contains(fp, "   ") {
			color.Set(color.FgRed)
			log.Printf("Warning: the file %s contains 3 consecutive blanks which is not allowed. The file not be archived.", fp)
			color.Unset()
			if illegalFileNamesCounter != nil {
				*illegalFileNamesCounter++
			}
			return false
		}
		return true
	}
}
