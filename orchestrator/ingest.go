package orchestrator

import (
	"bufio"
	"crypto/tls"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/SwissOpenEM/globus"
	"github.com/fatih/color"
	"github.com/paulscherrerinstitute/scicat-cli/v3/cmd/cliutils"
	"github.com/paulscherrerinstitute/scicat-cli/v3/datasetIngestor"
	"github.com/paulscherrerinstitute/scicat-cli/v3/datasetUtils"
	"github.com/spf13/cobra"
)

// === Dependency Injection Function Signatures ===

type DatasetRegistrar func(client *http.Client, apiServer string, metaDataMap map[string]interface{}, fullFileArray []datasetIngestor.Datafile, user map[string]string) (string, error)

type AttachmentAdder func(client *http.Client, apiServer, datasetId string, metaDataMap map[string]interface{}, token, filename, caption string) error

type AvailabilityChecker func(username, rsyncServer, sourceFolder string, output io.Writer) (error, error)

type SourceFolderTester func(datasetPaths []string, client *http.Client, apiServer, token string) (datasetIngestor.DatasetQuery, error)

var (
	checkForNewVersionFn          = datasetUtils.CheckForNewVersion
	checkForServiceAvailabilityFn = datasetUtils.CheckForServiceAvailability
	authenticateFn                = cliutils.Authenticate
	readAndCheckMetadataFn        = datasetIngestor.ReadAndCheckMetadata
	testForExistingSourceFolderFn = datasetIngestor.TestForExistingSourceFolder
	checkCentralAvailabilityFn    = datasetIngestor.CheckDataCentrallyAvailableSsh
	updateMetaDataFn              = datasetIngestor.UpdateMetaData
	resetUpdatedMetaDataFn        = datasetIngestor.ResetUpdatedMetaData
	ingestDatasetFn               = datasetIngestor.IngestDataset
	addAttachmentFn               = datasetIngestor.AddAttachment
	createArchivalJobFn           = datasetUtils.CreateArchivalJob
)

// IngestConfig encapsulates all parameter inputs parsed from CLI flags
type IngestConfig struct {
	EnvConfig                 cliutils.InputEnvironmentConfig
	IngestFlag                bool
	NoninteractiveFlag        bool
	Userpass                  string
	Token                     string
	Oidc                      bool
	CopyFlag                  bool
	NocopyFlag                bool
	TransferTypeFlag          string
	Tapecopies                int
	AutoarchiveFlag           bool
	Linkfiles                 string
	AllowExistingSourceFolder bool
	AddAttachment             string
	AddCaption                string
	ShowVersion               bool
	GlobusCfgFlag             string
	GlobusCfgChanged          bool
	RemoteFileScan            bool
}

// DatasetArgs holds the evaluated CLI positional arguments
type DatasetArgs struct {
	MetadataFile       string
	DatasetFileListTxt string
	FolderListingTxt   string
	AbsFileListing     string
}

// FileContext captures state results from file collection passes
type FileContext struct {
	FullFileArray []datasetIngestor.Datafile
	StartTime     time.Time
	EndTime       time.Time
	Owner         string
	NumFiles      int64
	TotalSize     int64
}

// IngestContext groups runtime dependencies together to avoid parameter bloat
type IngestContext struct {
	Cmd           *cobra.Command
	Cfg           IngestConfig
	Client        *http.Client
	APIServer     string
	RSYNCServer   string
	User          map[string]string
	AccessGroups  []string
	TransferFiles func(cliutils.TransferParams) (bool, error)
	GlobusClient  globus.GlobusClient
	GConfig       cliutils.GlobusConfig
	Scanner       *bufio.Scanner
}

// GlobalCounters manages cross-target metrics tracking safely
type GlobalCounters struct {
	SkipSymlinks     *string
	SkippedLinks     *uint
	IllegalFileNames *uint
	EmptyDatasets    *int
	TooLargeDatasets *int
	ArchivableList   *[]string
}

// ParseConfig builds the IngestConfig straight from Cobra flags
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
		IngestFlag:                cliutils.GetCobraBoolFlag(cmd, "ingest"),
		NoninteractiveFlag:        cliutils.GetCobraBoolFlag(cmd, "noninteractive"),
		Userpass:                  cliutils.GetCobraStringFlag(cmd, "user"),
		Token:                     cliutils.GetCobraStringFlag(cmd, "token"),
		Oidc:                      cliutils.GetCobraBoolFlag(cmd, "oidc"),
		CopyFlag:                  cliutils.GetCobraBoolFlag(cmd, "copy"),
		NocopyFlag:                cliutils.GetCobraBoolFlag(cmd, "nocopy"),
		TransferTypeFlag:          cliutils.GetCobraStringFlag(cmd, "transfer-type"),
		Tapecopies:                cliutils.GetCobraIntFlag(cmd, "tapecopies"),
		AutoarchiveFlag:           cliutils.GetCobraBoolFlag(cmd, "autoarchive"),
		Linkfiles:                 cliutils.GetCobraStringFlag(cmd, "linkfiles"),
		AllowExistingSourceFolder: cliutils.GetCobraBoolFlag(cmd, "allowexistingsource"),
		AddAttachment:             cliutils.GetCobraStringFlag(cmd, "addattachment"),
		AddCaption:                cliutils.GetCobraStringFlag(cmd, "addcaption"),
		ShowVersion:               cliutils.GetCobraBoolFlag(cmd, "version"),
		GlobusCfgFlag:             cliutils.GetCobraStringFlag(cmd, "globus-cfg"),
		GlobusCfgChanged:          cmd.Flags().Lookup("globus-cfg").Changed,
		RemoteFileScan:            cliutils.GetCobraBoolFlag(cmd, "remotefilescan"),
	}
}

// ParseAndValidateArgs processes raw string CLI positional arguments
func ParseAndValidateArgs(args []string) DatasetArgs {
	dArgs := DatasetArgs{
		MetadataFile: args[0],
	}
	if len(args) == 2 {
		argFileName := filepath.Base(args[1])
		if argFileName == "folderlisting.txt" {
			dArgs.FolderListingTxt = args[1]
		} else {
			dArgs.DatasetFileListTxt = args[1]
			dArgs.AbsFileListing, _ = filepath.Abs(args[1])
		}
	}
	return dArgs
}

func ParseAndValidateSeparatorArgs(args []string) (DatasetArgs, error) {
	if len(args) == 1 && strings.Contains(args[0], ":") {
		parts := strings.SplitN(args[0], ":", 2)
		metaFile := parts[0]
		listFile := parts[1]

		dArgs := DatasetArgs{MetadataFile: metaFile}
		argFileName := filepath.Base(listFile)
		if argFileName == "folderlisting.txt" {
			dArgs.FolderListingTxt = listFile
		} else {
			dArgs.DatasetFileListTxt = listFile
			dArgs.AbsFileListing, _ = filepath.Abs(listFile)
		}
		return dArgs, nil
	}
	return DatasetArgs{}, fmt.Errorf("invalid arguments")
}

func ParseAndValidateAllArgs(args []string) ([]DatasetArgs, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("no execution arguments provided")
	}

	var allArgs []DatasetArgs

	// 1. Check for legacy space-separated behavior at the very start (len == 2, no colon)
	if len(args) == 2 && !strings.Contains(args[0], ":") && !strings.Contains(args[1], ":") {
		return []DatasetArgs{ParseAndValidateArgs(args)}, nil
	}

	// 2. Otherwise, treat everything as a sequence of 1-arg chunks (colon-separated or standalone JSON)
	for i, arg := range args {
		if dArgs, err := ParseAndValidateSeparatorArgs([]string{arg}); err == nil {
			allArgs = append(allArgs, dArgs)
			continue
		}

		if strings.HasSuffix(strings.ToLower(arg), ".json") {
			allArgs = append(allArgs, ParseAndValidateArgs([]string{arg}))
			continue
		}

		return nil, fmt.Errorf("invalid argument at position %d: expected metadata.json:list.txt or standalone JSON, got '%s'", i+1, arg)
	}

	return allArgs, nil
}

// SetupTransferStrategy configures execution blocks or client initializations per transfer choice
func SetupTransferStrategy(cfg IngestConfig) (func(cliutils.TransferParams) (bool, error), globus.GlobusClient, cliutils.GlobusConfig) {
	transferType, err := cliutils.ConvertToTransferType(cfg.TransferTypeFlag)
	if err != nil {
		log.Fatalln(err)
	}

	var transferFiles func(params cliutils.TransferParams) (archivable bool, err error)
	var globusClient globus.GlobusClient
	var gConfig cliutils.GlobusConfig

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
				log.Fatalln("can't find executable path:", err)
			}
			globusConfigPath = filepath.Join(filepath.Dir(execPath), "globus.yaml")
		}

		globusClient, gConfig, err = cliutils.GlobusLogin(globusConfigPath)
		if err != nil {
			log.Fatalln("couldn't create globus client:", err)
		}

		if cfg.AutoarchiveFlag {
			log.Fatalln("Cannot autoarchive when transferring via Globus due to the transfer happening asynchronously. Use the \"globusCheckTransfer\" command to archive them")
		}
	}

	return transferFiles, globusClient, gConfig
}

// ResolveDatasetPaths builds the collection of directory target paths from lines and targets
func ResolveDatasetPaths(metadataSourceFolder string, folderListingTxt string) []string {
	var datasetPaths []string
	if folderListingTxt == "" {
		return append(datasetPaths, metadataSourceFolder)
	}

	folderlist, err := os.ReadFile(folderListingTxt)
	if err != nil {
		log.Fatal(err)
	}
	lines := strings.Split(string(folderlist), "\n")
	for _, line := range lines {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.Split(line, "/")
		if len(parts) > 3 && parts[3] == "data" {
			realSourceFolder, err := filepath.EvalSymlinks(line)
			if err != nil {
				log.Fatalf("Failed to find canonical form of sourceFolder:%v %v\n", line, err)
			}
			color.Set(color.FgYellow)
			log.Printf("Transform sourceFolder %v to canonical form: %v\n", line, realSourceFolder)
			color.Unset()
			datasetPaths = append(datasetPaths, realSourceFolder)
		} else {
			datasetPaths = append(datasetPaths, line)
		}
	}
	return datasetPaths
}

// GuardExistingSourceFolders runs matching target checks and prompts users before structural overrides
func GuardExistingSourceFolders(
	scanner *bufio.Scanner,
	datasetPaths []string,
	client *http.Client,
	apiServer, token string,
	allowExisting, flagChanged bool,
	testFn SourceFolderTester,
) {
	log.Println("Testing for existing source folders...")
	foundList, err := testFn(datasetPaths, client, apiServer, token)
	if err != nil {
		log.Fatal(err)
	}

	color.Set(color.FgYellow)
	if len(foundList) > 0 {
		fmt.Println("Warning! The following datasets have been found with the same sourceFolders: ")
	} else {
		log.Println("Finished testing for existing source folders.")
	}
	for _, element := range foundList {
		fmt.Printf("  - PID: \"%s\", sourceFolder: \"%s\"\n", element.Pid, element.SourceFolder)
	}
	color.Unset()

	if !allowExisting && len(foundList) > 0 {
		if !flagChanged {
			log.Printf("Do you want to ingest the corresponding new datasets nevertheless (y/N) ? ")
			scanner.Scan()
			if scanner.Text() != "y" {
				log.Fatalln("Aborted.")
			}
		} else {
			log.Fatalln("Existing sourceFolders are not allowed. Aborted.")
		}
	}
}

// GatherFiles collects local target files while tracking symlinks and structural parameters
func GatherFiles(datasetSourceFolder, datasetFileListTxt string, skipSymlinks *string, skippedLinks, illegalFileNames *uint) (FileContext, error) {
	if !(*skipSymlinks == "sA" || *skipSymlinks == "kA" || *skipSymlinks == "dA") {
		*skipSymlinks = ""
	}

	localSymlinkCallback := CreateLocalSymlinkCallbackForFileLister(skipSymlinks, skippedLinks)
	localFilepathFilterCallback := CreateLocalFilenameFilterCallback(illegalFileNames)

	log.Printf("Getting filelist for \"%s\"...\n", datasetSourceFolder)
	fullFileArray, startTime, endTime, owner, numFiles, totalSize, err :=
		datasetIngestor.GetLocalFileList(datasetSourceFolder, datasetFileListTxt, localSymlinkCallback, localFilepathFilterCallback)

	return FileContext{
		FullFileArray: fullFileArray,
		StartTime:     startTime,
		EndTime:       endTime,
		Owner:         owner,
		NumFiles:      numFiles,
		TotalSize:     totalSize,
	}, err
}

// VerifyCentralAvailability handles target presence checks and structural interaction loops
func VerifyCentralAvailability(
	cfg IngestConfig,
	rsyncServer, datasetSourceFolder string,
	user map[string]string,
	accessGroups []string,
	checkFn AvailabilityChecker,
) bool {
	log.Println("Checking if data is centrally available...")
	sshErr, otherErr := checkFn(user["username"], rsyncServer, datasetSourceFolder, os.Stdout)
	if otherErr != nil {
		log.Fatalln("Cannot check if data is centrally available:", otherErr)
	}

	if sshErr != nil {
		color.Set(color.FgYellow)
		log.Printf("The source folder %v is not centrally available.\nThe data must first be copied.\n ", datasetSourceFolder)
		color.Unset()

		if len(accessGroups) == 0 {
			color.Set(color.FgRed)
			log.Println("For copying, you must use a personal account. Beamline accounts are not supported.")
			color.Unset()
			os.Exit(1)
		}
		if !cfg.NoninteractiveFlag {
			log.Printf("Do you want to continue (Y/n)? ")
			scanner := bufio.NewScanner(os.Stdin)
			scanner.Scan()
			if scanner.Text() == "n" {
				log.Fatalln("Further ingests interrupted because copying is needed, but no copy wanted.")
			}
		}
		return true
	}

	log.Println("Data is present centrally.")
	return false
}

// InitializeLifecycleFields updates lifecycle maps depending on local storage status
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

// RegisterDatasetWithCatalog sends the metadata map array directly to the backend service
func RegisterDatasetWithCatalog(
	client *http.Client,
	apiServer string,
	metaDataMap map[string]interface{},
	fileCtx FileContext,
	user map[string]string,
	cfg IngestConfig,
	ingestFn DatasetRegistrar,
	attachFn AttachmentAdder,
) string {
	log.Println("Ingesting dataset...")
	datasetId, err := ingestFn(client, apiServer, metaDataMap, fileCtx.FullFileArray, user)
	if err != nil {
		log.Fatal("Couldn't ingest dataset:", err)
	}
	log.Println("Dataset created:", datasetId)

	if cfg.AddAttachment != "" {
		log.Println("Adding attachment...")
		err := attachFn(client, apiServer, datasetId, metaDataMap, user["accessToken"], cfg.AddAttachment, cfg.AddCaption)
		if err != nil {
			log.Println("Couldn't add attachment:", err)
		}
		log.Printf("Attachment file %v added to dataset %v\n", cfg.AddAttachment, datasetId)
	}
	return datasetId
}

// ExecuteFileTransfer invokes the assigned transfer hooks to route files asynchronously or synchronously
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
	var filePathList []string
	var isSymlinkList []bool

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

// RunIngestionPipeline coordinates the parsed runtime flags and drives the continuous ingestion loops
func RunIngestionPipeline(cmd *cobra.Command, args []string, version string) {
	var tooLargeDatasets = 0
	var emptyDatasets = 0

	var client = &http.Client{
		Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: false}},
		Timeout:   120 * time.Second,
	}

	cfg := ParseConfig(cmd)
	APIServer := cfg.EnvConfig.ResolveAPIServer()
	RSYNCServer := cfg.EnvConfig.ResolveRSYNCServer()

	transferFiles, globusClient, gConfig := SetupTransferStrategy(cfg)

	if datasetUtils.TestFlags != nil {
		datasetUtils.TestFlags(map[string]interface{}{
			"ingest":              cfg.IngestFlag,
			"testenv":             cfg.EnvConfig.TestenvFlag,
			"devenv":              cfg.EnvConfig.DevenvFlag,
			"localenv":            cfg.EnvConfig.LocalenvFlag,
			"tunnelenv":           cfg.EnvConfig.TunnelenvFlag,
			"scicat-url":          cfg.EnvConfig.ScicatUrl,
			"rsync-url":           cfg.EnvConfig.RsyncUrl,
			"noninteractive":      cfg.NoninteractiveFlag,
			"user":                cfg.Userpass,
			"token":               cfg.Token,
			"copy":                cfg.CopyFlag,
			"nocopy":              cfg.NocopyFlag,
			"tapecopies":          cfg.Tapecopies,
			"autoarchive":         cfg.AutoarchiveFlag,
			"linkfiles":           cfg.Linkfiles,
			"allowexistingsource": cfg.AllowExistingSourceFolder,
			"addattachment":       cfg.AddAttachment,
			"addcaption":          cfg.AddCaption,
			"version":             cfg.ShowVersion,
		})
		return
	}

	jobTargets, err := ParseAndValidateAllArgs(args)
	if err != nil {
		log.Fatal(err)
	}

	if datasetUtils.TestArgs != nil {
		var testInspectionBlock []interface{}
		for _, target := range jobTargets {
			testInspectionBlock = append(testInspectionBlock, target.MetadataFile, target.DatasetFileListTxt, target.FolderListingTxt)
		}
		datasetUtils.TestArgs(testInspectionBlock)
		return
	}

	if cfg.ShowVersion {
		fmt.Printf("%s\n", version)
		return
	}

	checkForNewVersionFn(client, "datasetIngestor", version)
	checkForServiceAvailabilityFn(client, cfg.EnvConfig.TestenvFlag, cfg.AutoarchiveFlag)

	user, accessGroups, err := authenticateFn(cliutils.RealAuthenticator{}, client, APIServer, cfg.Userpass, cfg.Token, cfg.Oidc)
	if err != nil {
		log.Fatal(err)
	}

	scanner := bufio.NewScanner(os.Stdin)

	if cfg.NocopyFlag {
		cfg.CopyFlag = false
	}

	skipSymlinks := ""
	if cmd.Flags().Changed("linkfiles") {
		switch cfg.Linkfiles {
		case "delete":
			skipSymlinks = "sA"
		case "keep":
			skipSymlinks = "kA"
		default:
			skipSymlinks = "dA"
		}
	}

	var skippedLinks uint = 0
	var illegalFileNames uint = 0
	var archivableDatasetList []string
	var archivableDatasetListOwnerGroup string

	// Build parameter context blocks
	ictx := IngestContext{
		Cmd:           cmd,
		Cfg:           cfg,
		Client:        client,
		APIServer:     APIServer,
		RSYNCServer:   RSYNCServer,
		User:          user,
		AccessGroups:  accessGroups,
		TransferFiles: transferFiles,
		GlobusClient:  globusClient,
		GConfig:       gConfig,
		Scanner:       scanner,
	}

	counters := GlobalCounters{
		SkipSymlinks:     &skipSymlinks,
		SkippedLinks:     &skippedLinks,
		IllegalFileNames: &illegalFileNames,
		EmptyDatasets:    &emptyDatasets,
		TooLargeDatasets: &tooLargeDatasets,
		ArchivableList:   &archivableDatasetList,
	}

	// Dynamic sequence iteration loop
	for _, dArgs := range jobTargets {
		ownerGroup, err := IngestTarget(ictx, counters, dArgs)
		if err != nil {
			log.Fatal(err)
		}
		if archivableDatasetListOwnerGroup == "" && ownerGroup != "" {
			archivableDatasetListOwnerGroup = ownerGroup
		}
	}

	if !cfg.IngestFlag {
		color.Set(color.FgRed)
		log.Printf("Note: you run in 'dry' mode to simply to check data consistency. Use the --ingest flag to really ingest datasets.")
	}

	if emptyDatasets > 0 || tooLargeDatasets > 0 {
		color.Set(color.FgRed)
		log.Printf("Errors encountered with dataset layouts. Job canceled.")
		color.Unset()
		os.Exit(1)
	}

	if cfg.AutoarchiveFlag && cfg.IngestFlag && len(archivableDatasetList) > 0 {
		if archivableDatasetListOwnerGroup == "" {
			log.Fatal("can't recover ownerGroup for archival submission.")
		}
		log.Printf("Submitting Archive Job for the ingested datasets.\n")
		jobId, err := createArchivalJobFn(client, APIServer, user, archivableDatasetListOwnerGroup, archivableDatasetList, &cfg.Tapecopies, nil)
		if err != nil {
			color.Set(color.FgRed)
			log.Printf("Could not create the archival job: %s\n", err.Error())
			color.Unset()
		}
		log.Println("Submitted job:", jobId)
	}

	for i := 0; i < len(archivableDatasetList); i++ {
		fmt.Println(archivableDatasetList[i])
	}
}

// IngestTarget handles folder resolution, validation, and execution for a single target unit block
func IngestTarget(ctx IngestContext, counters GlobalCounters, dArgs DatasetArgs) (string, error) {
	var originalMap = make(map[string]string)

	metaDataMap, metadataSourceFolder, beamlineAccount, err := readAndCheckMetadataFn(
		ctx.Client, ctx.APIServer, dArgs.MetadataFile, ctx.User, ctx.AccessGroups,
	)
	if err != nil {
		return "", fmt.Errorf("metadata file error for %s: %w", dArgs.MetadataFile, err)
	}

	ownerGroup, _ := metaDataMap["ownerGroup"].(string)
	datasetPaths := ResolveDatasetPaths(metadataSourceFolder, dArgs.FolderListingTxt)

	GuardExistingSourceFolders(
		ctx.Scanner, datasetPaths, ctx.Client, ctx.APIServer, ctx.User["accessToken"],
		ctx.Cfg.AllowExistingSourceFolder, ctx.Cmd.Flags().Changed("allowexistingsource"),
		testForExistingSourceFolderFn,
	)

	checkCentralAvailability := !(ctx.Cmd.Flags().Changed("copy") || ctx.Cmd.Flags().Changed("nocopy") || beamlineAccount || ctx.Cfg.CopyFlag)

	for _, datasetSourceFolder := range datasetPaths {
		if datasetSourceFolder == "" {
			continue
		}

		log.Printf("===== Ingesting: \"%s\" =====\n", datasetSourceFolder)
		metaDataMap["sourceFolder"] = datasetSourceFolder

		fileCtx, err := GatherFiles(datasetSourceFolder, dArgs.DatasetFileListTxt, counters.SkipSymlinks, counters.SkippedLinks, counters.IllegalFileNames)
		if err != nil {
			return "", fmt.Errorf("failed gathering filelist for directory \"%s\": %w", datasetSourceFolder, err)
		}

		if fileCtx.TotalSize == 0 {
			*counters.EmptyDatasets++
			continue
		}
		if fileCtx.NumFiles > cliutils.TOTAL_MAXFILES {
			*counters.TooLargeDatasets++
			continue
		}

		if ctx.Cfg.Tapecopies == 2 {
			log.Println("Note: this dataset, if archived, will be copied to two tape copies")
		}

		updateMetaDataFn(ctx.Client, ctx.APIServer, ctx.User, originalMap, metaDataMap, fileCtx.StartTime, fileCtx.EndTime, fileCtx.Owner, ctx.Cfg.Tapecopies)

		requiresCopy := ctx.Cfg.CopyFlag
		if checkCentralAvailability {
			requiresCopy = VerifyCentralAvailability(ctx.Cfg, ctx.RSYNCServer, datasetSourceFolder, ctx.User, ctx.AccessGroups, checkCentralAvailabilityFn)
		}

		if !ctx.Cfg.IngestFlag {
			resetUpdatedMetaDataFn(originalMap, metaDataMap)
			continue
		}

		archivable := InitializeLifecycleFields(metaDataMap, requiresCopy)
		datasetId := ""
		var registrarFn DatasetRegistrar = ingestDatasetFn
		if ctx.Cfg.RemoteFileScan {
			if lifecycle, ok := metaDataMap["datasetlifecycle"].(map[string]interface{}); ok {
				lifecycle["archiveStatusMessage"] = "origDatablocksNotYetAvailable"
			}
			registrarFn = func(client *http.Client, apiServer string, metaDataMap map[string]interface{}, fullFileArray []datasetIngestor.Datafile, user map[string]string) (string, error) {
				return datasetIngestor.CreateDataset(client, apiServer, metaDataMap, user)
			}
		}
		datasetId = RegisterDatasetWithCatalog(ctx.Client, ctx.APIServer, metaDataMap, fileCtx, ctx.User, ctx.Cfg, registrarFn, addAttachmentFn)
		if requiresCopy {
			archivable = ExecuteFileTransfer(
				ctx.Client, ctx.APIServer, ctx.RSYNCServer, datasetId, datasetSourceFolder,
				dArgs.AbsFileListing, ctx.User, fileCtx, ctx.TransferFiles, ctx.GlobusClient, ctx.GConfig, ctx.Cfg.TransferTypeFlag, !ctx.Cfg.RemoteFileScan,
			)
		}

		if archivable {
			*counters.ArchivableList = append(*counters.ArchivableList, datasetId)
		}

		resetUpdatedMetaDataFn(originalMap, metaDataMap)
	}

	return ownerGroup, nil
}

// CreateLocalSymlinkCallbackForFileLister isolates interactive symlink evaluation loops
func CreateLocalSymlinkCallbackForFileLister(skipSymlinks *string, skippedLinks *uint) func(symlinkPath string, sourceFolder string) (bool, error) {
	scanner := bufio.NewScanner(os.Stdin)
	return func(symlinkPath string, sourceFolder string) (bool, error) {
		keep := true
		pointee, _ := os.Readlink(symlinkPath)
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
		if *skipSymlinks == "ka" || *skipSymlinks == "kA" {
			keep = true
		} else if *skipSymlinks == "sa" || *skipSymlinks == "sA" {
			keep = false
		} else if *skipSymlinks == "da" || *skipSymlinks == "dA" {
			keep = strings.HasPrefix(pointee, sourceFolder)
		} else {
			color.Set(color.FgYellow)
			log.Printf("Warning: the file %s is a link pointing to %v.", symlinkPath, pointee)
			color.Unset()
			log.Printf("\nDo you want to keep the link in dataset or skip it (D(efault)/k(eep)/s(kip) ?")
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

// CreateLocalFilenameFilterCallback checks security strings for illegal runes
func CreateLocalFilenameFilterCallback(illegalFileNamesCounter *uint) func(filepath string) bool {
	return func(filepath string) (keep bool) {
		keep = true
		if strings.ContainsAny(filepath, "*\\") {
			color.Set(color.FgRed)
			log.Printf("Warning: the file %s contains illegal characters like *,\\ and will not be archived.", filepath)
			color.Unset()
			if illegalFileNamesCounter != nil {
				*illegalFileNamesCounter++
			}
			keep = false
		}
		if keep && strings.Contains(filepath, "   ") {
			color.Set(color.FgRed)
			log.Printf("Warning: the file %s contains 3 consecutive blanks which is not allowed. The file not be archived.", filepath)
			color.Unset()
			if illegalFileNamesCounter != nil {
				*illegalFileNamesCounter++
			}
			keep = false
		}
		return keep
	}
}
