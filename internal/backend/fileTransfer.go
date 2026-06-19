package backend

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/SwissOpenEM/globus"
	"github.com/paulscherrerinstitute/scicat-cli/v3/cmd/cliutils"
	"github.com/paulscherrerinstitute/scicat-cli/v3/datasetIngestor"
)

type FileService struct {
	User                 map[string]string
	MetadataSourceFolder string
	FolderListingTxt     string
	AbsFilelistPath      string

	TransferFiles func(params cliutils.TransferParams) (archivable bool, err error)
	GlobusClient  globus.GlobusClient
	GlobusConfig  cliutils.GlobusConfig
	RsyncServer   string

	TotalSkippedLinks     uint
	TotalIllegalFileNames uint
	EmptyDatasetsCount    uint
	TooLargeDatasetsCount uint

	activeTransferType cliutils.TransferType
}

func NewFileService(user map[string]string, metadataSourceFolder string, folderListingTxt string, absFilelistPath string, rsyncServer string) *FileService {
	return &FileService{
		User:                 user,
		MetadataSourceFolder: metadataSourceFolder,
		FolderListingTxt:     folderListingTxt,
		AbsFilelistPath:      absFilelistPath,
		RsyncServer:          rsyncServer,
	}
}

func (fs *FileService) InitializeTransferStrategy(
	transferTypeFlag string,
	globusCfgFlag string,
	globusCfgChanged bool,
	autoarchiveFlag bool,
) error {
	transferType, err := cliutils.ConvertToTransferType(transferTypeFlag)
	if err != nil {
		return fmt.Errorf("invalid transfer type configuration: %w", err)
	}

	fs.activeTransferType = transferType

	switch transferType {
	case cliutils.Ssh:
		fs.TransferFiles = cliutils.SshTransfer

	case cliutils.Globus:
		fs.TransferFiles = cliutils.GlobusTransfer

		var globusConfigPath string
		if globusCfgChanged {
			globusConfigPath = globusCfgFlag
		} else {
			execPath, err := os.Executable()
			if err != nil {
				return fmt.Errorf("can't find executable path for Globus config: %w", err)
			}
			globusConfigPath = filepath.Join(filepath.Dir(execPath), "globus.yaml")
		}

		client, config, err := cliutils.GlobusLogin(globusConfigPath)
		if err != nil {
			return fmt.Errorf("couldn't create globus client: %w", err)
		}

		fs.GlobusClient = client
		fs.GlobusConfig = config

		if autoarchiveFlag {
			return fmt.Errorf("cannot autoarchive when transferring via Globus due to the transfer happening asynchronously. Use the \"globusCheckTransfer\" command to archive them")
		}
	}

	return nil
}

func (fs *FileService) ResolveDatasetPaths() []string {
	var datasetPaths []string
	if fs.FolderListingTxt == "" {
		return append(datasetPaths, fs.MetadataSourceFolder)
	}

	folderlist, err := os.ReadFile(fs.FolderListingTxt)
	if err != nil {
		log.Fatal(err)
	}

	lines := strings.Split(string(folderlist), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		var parts = strings.Split(line, "/")
		if len(parts) > 3 && parts[3] == "data" {
			realSourceFolder, err := filepath.EvalSymlinks(line)
			if err != nil {
				log.Fatalf("Failed to find canonical form of sourceFolder: %v %v\n", line, err)
			}
			datasetPaths = append(datasetPaths, realSourceFolder)
		} else {
			datasetPaths = append(datasetPaths, line)
		}
	}
	return datasetPaths
}

func (fs *FileService) ScanAndVerifyFiles(
	sourceFolder string,
	fileListTxt string,
	localSymlinkCallback func(string, string) (bool, error),
) ([]datasetIngestor.Datafile, time.Time, time.Time, string, bool, error) {
	log.Printf("Scanning files in dataset: %s", sourceFolder)

	localFilepathFilterCallback := fs.GetFilenameFilterCallback()

	fullFileArray, startTime, endTime, owner, numFiles, totalSize, err :=
		datasetIngestor.GetLocalFileList(sourceFolder, fileListTxt, localSymlinkCallback, localFilepathFilterCallback)
	if err != nil {
		return nil, time.Time{}, time.Time{}, "", false, fmt.Errorf("gathering file list failed for %s: %w", sourceFolder, err)
	}

	log.Printf("The dataset contains %v files with a total size of %v bytes.\n", numFiles, totalSize)

	if totalSize == 0 {
		fs.EmptyDatasetsCount++
		log.Printf("Warning: \"%s\" dataset cannot be ingested - contains no files\n", sourceFolder)
		return nil, time.Time{}, time.Time{}, "", false, fmt.Errorf("empty dataset structure found")
	}
	if numFiles > cliutils.TOTAL_MAXFILES {
		fs.TooLargeDatasetsCount++
		log.Printf("Warning: \"%s\" dataset cannot be ingested - too many files: has %d, max. %d\n", sourceFolder, numFiles, cliutils.TOTAL_MAXFILES)
		return nil, time.Time{}, time.Time{}, "", false, fmt.Errorf("file count bound exceptions: limit exceeded")
	}

	return fullFileArray, startTime, endTime, owner, true, nil
}

func (fs *FileService) AuditCentralDataAvailability(folder string) (bool, error) {
	log.Println("Checking if data is centrally available...")

	sshErr, otherErr := datasetIngestor.CheckDataCentrallyAvailableSsh(fs.User["username"], fs.RsyncServer, folder, os.Stdout)
	if otherErr != nil {
		return false, fmt.Errorf("cannot verify central database storage arrays: %w", otherErr)
	}
	if sshErr == nil {
		log.Println("Data is present centrally.")
		return false, nil
	}

	return true, sshErr
}

func (fs *FileService) TransferDatasetFiles(
	datasetId string,
	datasetSourceFolder string,
	fullFileArray []datasetIngestor.Datafile,
	client *http.Client,
	ApiServer string,

) (bool, error) {
	if fs.TransferFiles == nil {
		return false, fmt.Errorf("no file transfer strategy was initialized")
	}

	params := cliutils.TransferParams{
		DatasetId:           datasetId,
		DatasetSourceFolder: datasetSourceFolder,
	}

	switch fs.activeTransferType {
	case cliutils.Ssh:
		params.SshParams = cliutils.SshParams{
			User:            fs.User,
			RsyncServer:     fs.RsyncServer,
			AbsFilelistPath: fs.AbsFilelistPath,
		}

	case cliutils.Globus:
		var filePathList []string
		var isSymlinkList []bool
		for _, file := range fullFileArray {
			filePathList = append(filePathList, file.Path)
			isSymlinkList = append(isSymlinkList, file.IsSymlink)
		}

		params.GlobusParams = cliutils.GlobusParams{
			GlobusClient:   fs.GlobusClient,
			SrcCollection:  fs.GlobusConfig.SourceCollection,
			SrcPrefixPath:  fs.GlobusConfig.SourcePrefixPath,
			DestCollection: fs.GlobusConfig.DestinationCollection,
			DestPrefixPath: fs.GlobusConfig.DestinationPrefixPath,
			Filelist:       filePathList,
			IsSymlinkList:  isSymlinkList,
		}
	}

	return fs.TransferFiles(params)
}

func (fs *FileService) EvaluateSymlinkStrategy(hasChanged bool, linkfilesFlag string) string {
	if !hasChanged {
		return ""
	}
	switch linkfilesFlag {
	case "delete":
		return "sA"
	case "keep":
		return "kA"
	default:
		return "dA"
	}
}

func (fs *FileService) ResetLocalSymlinkStrategy(currentStrategy string) string {
	if currentStrategy == "sA" || currentStrategy == "kA" || currentStrategy == "dA" {
		return currentStrategy
	}
	return ""
}

func (fs *FileService) GetFilenameFilterCallback() func(string) bool {
	return func(filepath string) bool {
		keep := true

		if strings.ContainsAny(filepath, "*\\") {
			log.Printf("Warning: the file %s contains illegal characters like *,\\ and will not be archived.", filepath)
			fs.TotalIllegalFileNames++
			keep = false
		}

		if keep && strings.Contains(filepath, "   ") {
			log.Printf("Warning: the file %s contains 3 consecutive blanks which is not allowed. The file will not be archived.", filepath)
			fs.TotalIllegalFileNames++
			keep = false
		}

		return keep
	}
}

func (fs *FileService) EvaluateSymlink(
	symlinkPath string,
	sourceFolder string,
	strategy string,
	promptUserHandler func(warningMsg string, choicePrompt string) string,
) (bool, string) {
	keep := true
	pointee, _ := os.Readlink(symlinkPath)

	if !filepath.IsAbs(pointee) {
		symlinkAbs, err := filepath.Abs(filepath.Dir(symlinkPath))
		if err != nil {
			return false, strategy
		}
		pointeeAbs := filepath.Join(symlinkAbs, pointee)
		pointee, err = filepath.EvalSymlinks(pointeeAbs)
		if err != nil {
			log.Printf("Could not follow symlink for file:%v %v", pointeeAbs, err)
			keep = false
		}
	}

	if strategy == "ka" || strategy == "kA" {
		keep = true
	} else if strategy == "sa" || strategy == "sA" {
		keep = false
	} else if strategy == "da" || strategy == "dA" {
		keep = strings.HasPrefix(pointee, sourceFolder)
	} else {
		warningMsg := fmt.Sprintf("Warning: the file %s is a link pointing to %v.", symlinkPath, pointee)
		choicePrompt := fmt.Sprintf(`
    Please test if this link is meaningful and not pointing
    outside the sourceFolder %s. The default behaviour is to
    keep only internal links within a source folder.
    You can also specify that you want to apply the same answer to ALL
    subsequent links within the current dataset, by appending an a (dA,ka,sa).
    If you want to give the same answer even to all subsequent datasets
    in this command then specify a capital 'A', e.g. (dA,kA,sA)
    Do you want to keep the link in dataset or skip it (D(efault)/k(eep)/s(kip) ?`, sourceFolder)

		strategy = promptUserHandler(warningMsg, choicePrompt)
		if strategy == "" {
			strategy = "d"
		}

		if strategy == "d" || strategy == "dA" {
			keep = strings.HasPrefix(pointee, sourceFolder)
		} else {
			keep = (strategy != "s" && strategy != "sa" && strategy != "sA")
		}
	}

	if keep {
		log.Printf("You chose to keep the link %v -> %v.\n\n", symlinkPath, pointee)
	} else {
		fs.TotalSkippedLinks++
		log.Printf("You chose to remove the link %v -> %v.\n\n", symlinkPath, pointee)
	}

	return keep, strategy
}
