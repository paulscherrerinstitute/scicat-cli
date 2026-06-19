package cmd

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/fatih/color"
	"github.com/paulscherrerinstitute/scicat-cli/v3/cmd/cliutils"
	"github.com/paulscherrerinstitute/scicat-cli/v3/datasetIngestor"
	"github.com/paulscherrerinstitute/scicat-cli/v3/datasetUtils"
	"github.com/paulscherrerinstitute/scicat-cli/v3/internal/backend"
	"github.com/spf13/cobra"
)

// IngestConfig orchestrates all flag variations (behavioral, auth, and environment)
type IngestConfig struct {
	Ingest, NonInteractive, Copy, NoCopy, AutoArchive, AllowExisting, Version bool
	TransferType, LinkFiles, AddAttachment, AddCaption, GlobusCfg             string
	TapeCopies                                                                int
	EnvConfig                                                                 cliutils.InputEnvironmentConfig
	AuthOpts                                                                  backend.AuthOptions
}

// UI Utilities
func printWarning(m string) { color.Set(color.FgYellow); log.Println(m); color.Unset() }
func printError(m string)   { color.Set(color.FgRed); log.Println(m); color.Unset() }
func promptUser(m string, scanner *bufio.Scanner) string {
	log.Print(m)
	scanner.Scan()
	return scanner.Text()
}

// 1. parseArgs handles positional argument verification
func parseArgs(args []string) (string, string, string, string) {
	if len(args) <= 0 || len(args) >= 3 {
		log.Fatal("invalid number of args")
	}

	metadatafile := args[0]
	datasetFileListTxt := ""
	folderListingTxt := ""
	absFileListing := ""

	if len(args) == 2 {
		argFileName := filepath.Base(args[1])
		if argFileName == "folderlisting.txt" {
			folderListingTxt = args[1]
		} else {
			datasetFileListTxt = args[1]
			absFileListing, _ = filepath.Abs(datasetFileListTxt)
		}
	}
	return metadatafile, datasetFileListTxt, folderListingTxt, absFileListing
}

// 2. parseFlagsAndConfigs extracts and packages every single Cobra flag
func parseFlagsAndConfigs(cmd *cobra.Command) IngestConfig {
	envCfg := cliutils.InputEnvironmentConfig{
		TestenvFlag:   cliutils.GetCobraBoolFlag(cmd, "testenv"),
		DevenvFlag:    cliutils.GetCobraBoolFlag(cmd, "devenv"),
		TunnelenvFlag: cliutils.GetCobraBoolFlag(cmd, "tunnelenv"),
		LocalenvFlag:  cliutils.GetCobraBoolFlag(cmd, "localenv"),
		ScicatUrl:     cliutils.GetCobraStringFlag(cmd, "scicat-url"),
		RsyncUrl:      cliutils.GetCobraStringFlag(cmd, "rsync-url"),
	}

	return IngestConfig{
		Ingest:         cliutils.GetCobraBoolFlag(cmd, "ingest"),
		NonInteractive: cliutils.GetCobraBoolFlag(cmd, "noninteractive"),
		Copy:           cliutils.GetCobraBoolFlag(cmd, "copy"),
		NoCopy:         cliutils.GetCobraBoolFlag(cmd, "nocopy"),
		TransferType:   cliutils.GetCobraStringFlag(cmd, "transfer-type"),
		TapeCopies:     cliutils.GetCobraIntFlag(cmd, "tapecopies"),
		AutoArchive:    cliutils.GetCobraBoolFlag(cmd, "autoarchive"),
		LinkFiles:      cliutils.GetCobraStringFlag(cmd, "linkfiles"),
		AllowExisting:  cliutils.GetCobraBoolFlag(cmd, "allowexistingsource"),
		AddAttachment:  cliutils.GetCobraStringFlag(cmd, "addattachment"),
		AddCaption:     cliutils.GetCobraStringFlag(cmd, "addcaption"),
		Version:        cliutils.GetCobraBoolFlag(cmd, "version"),
		GlobusCfg:      cliutils.GetCobraStringFlag(cmd, "globus-cfg"),
		EnvConfig:      envCfg,
		AuthOpts: backend.AuthOptions{
			User:        cliutils.GetCobraStringFlag(cmd, "user"),
			Token:       cliutils.GetCobraStringFlag(cmd, "token"),
			Oidc:        cliutils.GetCobraBoolFlag(cmd, "oidc"),
			TestEnv:     envCfg.TestenvFlag,
			AutoArchive: cliutils.GetCobraBoolFlag(cmd, "autoarchive"),
		},
	}
}

// 3. instantiateServices resolves configurations and bootstraps the backend services
func instantiateServices(cmd *cobra.Command, cfg IngestConfig, folderListingTxt, absFileListing string) (*backend.TransportEngine, *backend.FileService, *backend.IngestService, *backend.UserSession) {
	transportEngine := backend.BootstrapTransportEngine(
		cfg.EnvConfig.ResolveAPIServer(),
		cfg.EnvConfig.ResolveRSYNCServer(),
	)

	userSession, err := transportEngine.InitializeSession(VERSION, cfg.AuthOpts)
	if err != nil {
		log.Fatalf("Initialization failed: %v", err)
	}

	// Read initial metadata to extract default source folder context
	_, metadataSourceFolder, _, err := datasetIngestor.ReadAndCheckMetadata(
		transportEngine.Client, transportEngine.APIServer, cmd.Flags().Args()[0], userSession.User, userSession.AccessGroups,
	)
	if err != nil {
		log.Fatal("Error in CheckMetadata function: ", err)
	}

	fileService := backend.NewFileService(userSession.User, metadataSourceFolder, folderListingTxt, absFileListing, transportEngine.RsyncServer)
	err = fileService.InitializeTransferStrategy(
		cfg.TransferType,
		cfg.GlobusCfg,
		cmd.Flags().Lookup("globus-cfg").Changed,
		cfg.AutoArchive,
	)
	if err != nil {
		log.Fatalln("Strategy allocation failed:", err)
	}

	ingestService := backend.NewIngestService(transportEngine, fileService)
	return transportEngine, fileService, ingestService, userSession
}

// 4. verifyExistingSources checks remote records and challenges collisions locally
func verifyExistingSources(cmd *cobra.Command, cfg IngestConfig, ingestService *backend.IngestService, paths []string, scanner *bufio.Scanner) {
	log.Println("Testing for existing source folders...")
	foundList, err := ingestService.CheckExistingSources(paths)
	if err != nil {
		log.Fatal(err)
	}
	if len(foundList) > 0 {
		printWarning("Warning! The following datasets have been found with the same sourceFolders:")
		for _, element := range foundList {
			fmt.Printf("  - PID: \"%s\", sourceFolder: \"%s\"\n", element.Pid, element.SourceFolder)
		}

		if !cfg.AllowExisting && cmd.Flags().Changed("allowexistingsource") {
			log.Fatalln("Existing sourceFolders are not allowed. Aborted.")
		}
		if !cfg.AllowExisting && !cmd.Flags().Changed("allowexistingsource") {
			if promptUser("Do you want to continue (y/N) ? ", scanner) != "y" {
				log.Fatalln("Aborted.")
			}
		}
	} else {
		log.Println("Finished testing for existing source folders.")
	}
}

// 5. scanAndVerifyDatasetFiles invokes the disk/symlink scanner logic path
func scanAndVerifyDatasetFiles(fileService *backend.FileService, cmd *cobra.Command, cfg IngestConfig, folder, datasetFileListTxt string, skipSymlinks *string) ([]datasetIngestor.Datafile, time.Time, time.Time, string, bool) {
	log.Printf("Scanning files in dataset %s", folder)
	*skipSymlinks = fileService.ResetLocalSymlinkStrategy(*skipSymlinks)
	localSymlinkCallback := createLocalSymlinkCallbackForFileLister(fileService, skipSymlinks)

	fullFileArray, startTime, endTime, owner, isValid, err := fileService.ScanAndVerifyFiles(
		folder,
		datasetFileListTxt,
		localSymlinkCallback,
	)
	if err != nil {
		log.Fatalf("Verification failed for \"%s\": %v", folder, err)
	}
	log.Println("File list collected.")
	return fullFileArray, startTime, endTime, owner, isValid
}

// 6. checkCentralAvailability evaluates whether remote copy flags must be flipped
func checkDataCentralAvailability(fileService *backend.FileService, cfg *IngestConfig, session *backend.UserSession, folder string, isBeamline, isCopyFlipped, isNoCopyFlipped bool, scanner *bufio.Scanner) bool {
	checkCentralAvailability := !(isCopyFlipped || isNoCopyFlipped || isBeamline || cfg.Copy)
	if !checkCentralAvailability {
		return cfg.Copy
	}

	log.Println("Checking if data is centrally available...")
	needsCopy, sshErr := fileService.AuditCentralDataAvailability(folder)
	if sshErr != nil && !needsCopy {
		log.Fatalln("Cannot check if data is centrally available:", sshErr)
	}
	if needsCopy {
		printWarning(fmt.Sprintf("The source folder %v is not centrally available.\nThe data must first be copied.\n ", folder))
		cfg.Copy = true

		if len(session.AccessGroups) == 0 {
			printError("For copying, you must use a personal account. Beamline accounts are not supported.")
			os.Exit(1)
		}

		if !cfg.NonInteractive {
			if promptUser("Do you want to continue (Y/n)? ", scanner) == "n" {
				log.Fatalln("Further ingests interrupted because copying is needed, but no copy wanted.")
			}
		}
	}
	return cfg.Copy
}

// 7. executeIngestionLoop runs across sorted staging directories
func executeIngestionLoop(cmd *cobra.Command, cfg IngestConfig, session *backend.UserSession, fileService *backend.FileService, ingestService *backend.IngestService, batch *backend.DatasetBatch, paths []string, datasetFileListTxt string, scanner *bufio.Scanner) {
	var originalMap = make(map[string]string)

	if cfg.NoCopy {
		cfg.Copy = false
	}

	_, ok := batch.MetaDataMap["ownerGroup"].(string)
	if !ok {
		log.Fatal("can't recover ownerGroup. This should normally be impossible as the checkMetadata function should've caught it already.")
	}

	skipSymlinks := fileService.EvaluateSymlinkStrategy(cmd.Flags().Changed("linkfiles"), cfg.LinkFiles)
	runtimeCfg := backend.DatasetIngestRuntimeConfig{
		Tapecopies:    cfg.TapeCopies,
		AddAttachment: cfg.AddAttachment,
		AddCaption:    cfg.AddCaption,
		RSYNCServer:   batch.MetaDataMap["rsyncServer"].(string), // resolved during batch prepare step
	}

	for _, datasetSourceFolder := range paths {
		if datasetSourceFolder == "" {
			continue
		}
		log.Printf("===== Ingesting: \"%s\" =====\n", datasetSourceFolder)
		batch.MetaDataMap["sourceFolder"] = datasetSourceFolder

		fileArray, start, end, owner, isValid := scanAndVerifyDatasetFiles(fileService, cmd, cfg, datasetSourceFolder, datasetFileListTxt, &skipSymlinks)
		if !isValid {
			continue
		}

		if cfg.TapeCopies == 2 {
			printWarning("Note: this dataset, if archived, will be copied to two tape copies")
		}

		cfg.Copy = checkDataCentralAvailability(
			fileService, &cfg, session, datasetSourceFolder, batch.BeamlineAccount,
			cmd.Flags().Changed("copy"), cmd.Flags().Changed("nocopy"), scanner,
		)

		if cfg.Ingest {
			_, err := ingestService.Ingest(batch, datasetSourceFolder, fileArray, start, end, owner, cfg.Copy, runtimeCfg)
			if err != nil {
				log.Fatalf("Ingestion sequence aborted: %v", err)
			}
		}
		datasetIngestor.ResetUpdatedMetaData(originalMap, batch.MetaDataMap)
	}
}

var datasetIngestorCmd = &cobra.Command{
	Use:   "datasetIngestor",
	Short: "Define and add a dataset to the SciCat datacatalog",
	Long: `Purpose: define and add a dataset to the SciCat datacatalog

This command must be run on the machine having access to the data
which comprises the dataset. It takes one or two input
files and creates the necessary messages which trigger
the creation of the corresponding datacatalog entries

For further help see "` + cliutils.MANUAL + `"`,
	Args: rangeArgsWithVersionException(1, 2),
	Run: func(cmd *cobra.Command, args []string) {
		var tooLargeDatasets = 0
		var emptyDatasets = 0
		var scanner = bufio.NewScanner(os.Stdin)

		// 1 & 2. Parse arguments and flags completely
		cfg := parseFlagsAndConfigs(cmd)
		metadatafile, datasetFileListTxt, folderListingTxt, absFileListing := parseArgs(args)

		// Verification hooks for testing frameworks
		if datasetUtils.TestFlags != nil {
			datasetUtils.TestFlags(map[string]interface{}{
				"ingest":              cfg.Ingest,
				"scicat-url":          cfg.EnvConfig.ScicatUrl,
				"rsync-url":           cfg.EnvConfig.RsyncUrl,
				"noninteractive":      cfg.NonInteractive,
				"user":                cfg.AuthOpts.User,
				"token":               cfg.AuthOpts.Token,
				"copy":                cfg.Copy,
				"nocopy":              cfg.NoCopy,
				"tapecopies":          cfg.TapeCopies,
				"autoarchive":         cfg.AutoArchive,
				"linkfiles":           cfg.LinkFiles,
				"allowexistingsource": cfg.AllowExisting,
				"addattachment":       cfg.AddAttachment,
				"addcaption":          cfg.AddCaption,
				"version":             cfg.Version,
			})
			return
		}
		if datasetUtils.TestArgs != nil {
			datasetUtils.TestArgs([]interface{}{metadatafile, datasetFileListTxt, folderListingTxt})
			return
		}
		if cfg.Version {
			fmt.Printf("%s\n", VERSION)
			return
		}

		// 3. Service Initialization
		transportEngine, fileService, ingestService, userSession := instantiateServices(cmd, cfg, folderListingTxt, absFileListing)

		batch, err := ingestService.PrepareBatch(metadatafile, absFileListing)
		if err != nil {
			log.Fatal(err)
		}
		// Explicit mapping injection so executeIngestionLoop reads current values correctly
		batch.MetaDataMap["rsyncServer"] = transportEngine.RsyncServer

		datasetPaths := fileService.ResolveDatasetPaths()

		// 4. Remote collision check
		verifyExistingSources(cmd, cfg, ingestService, datasetPaths, scanner)

		// 5, 6 & 7. Execution Context Loop execution wrapper
		executeIngestionLoop(cmd, cfg, userSession, fileService, ingestService, batch, datasetPaths, datasetFileListTxt, scanner)

		// Post Loop execution stats printing
		if fileService.EmptyDatasetsCount > 0 {
			printError(fmt.Sprintf("Total empty datasets skipped: %d", fileService.EmptyDatasetsCount))
		}
		if fileService.TooLargeDatasetsCount > 0 {
			printError(fmt.Sprintf("Total oversized datasets skipped: %d", fileService.TooLargeDatasetsCount))
		}
		if !cfg.Ingest {
			printError("Note: you run in 'dry' mode to simply to check data consistency. Use the --ingest flag to really ingest datasets.")
		}
		if emptyDatasets > 0 {
			printError(fmt.Sprintf("Number of datasets not stored because they are empty:%v\n. Please note that this will cancel any subsequent archive steps from this job !", emptyDatasets))
		}
		if tooLargeDatasets > 0 {
			printError(fmt.Sprintf("Number of datasets not stored because of too many files:%v\nPlease note that this will cancel any subsequent archive steps from this job !", tooLargeDatasets))
		}
		if fileService.TotalSkippedLinks > 0 {
			printWarning(fmt.Sprintf("Total number of link files skipped:%v", fileService.TotalSkippedLinks))
		}
		if fileService.TotalIllegalFileNames > 0 {
			printError(fmt.Sprintf("Number of files ignored because of illegal filenames:%v", fileService.TotalIllegalFileNames))
		}

		if emptyDatasets > 0 || tooLargeDatasets > 0 {
			os.Exit(1)
		}

		// Automatic archival processing step
		if cfg.AutoArchive && cfg.Ingest {
			archiveService := backend.NewArchiveService(transportEngine)
			_, err := archiveService.SubmitArchivalJob(batch.MetaDataMap["ownerGroup"].(string), ingestService.ArchivableDatasetIDs, cfg.TapeCopies)
			if err != nil {
				printError(fmt.Sprintf("Could not create the archival job for the ingested datasets: %s", err.Error()))
			}
		}

		for i := 0; i < len(ingestService.ArchivableDatasetIDs); i++ {
			fmt.Println(ingestService.ArchivableDatasetIDs[i])
		}
	},
}

func init() {
	rootCmd.AddCommand(datasetIngestorCmd)

	datasetIngestorCmd.Flags().Bool("ingest", false, "Defines if this command is meant to actually ingest data")
	datasetIngestorCmd.Flags().Bool("testenv", false, "Use test environment (qa) instead of production environment")
	datasetIngestorCmd.Flags().Bool("devenv", false, "Use development environment instead of production environment (developers only)")
	datasetIngestorCmd.Flags().Bool("localenv", false, "Use local environment instead of production environment (developers only)")
	datasetIngestorCmd.Flags().Bool("tunnelenv", false, "Use tunneled API server at port 5443 to access development instance (developers only)")
	datasetIngestorCmd.Flags().String("rsync-url", "", "Custom URL for the rsync server. It is a complementary parameter 'scicat-url', but is not required. When not given, the chosen environment's RSYNC server is used.")
	datasetIngestorCmd.Flags().Bool("noninteractive", false, "If set no questions will be asked and the default settings for all undefined flags will be assumed")
	datasetIngestorCmd.Flags().Bool("copy", false, "Defines if files should be copied from your local system to a central server before ingest")
	datasetIngestorCmd.Flags().Bool("nocopy", false, "Defines if files should *not* be copied from your local system to a central server before ingest")
	datasetIngestorCmd.Flags().String("transfer-type", "ssh", "Selects the transfer type to be used for transferring files. Available options: \"ssh\", \"globus\"")
	datasetIngestorCmd.Flags().Int("tapecopies", 0, "Number of tapecopies to be used for archiving")
	datasetIngestorCmd.Flags().Bool("autoarchive", false, "Option to create archive job automatically after ingestion")
	datasetIngestorCmd.Flags().String("linkfiles", "keepInternalOnly", "Define what to do with symbolic links: (keep|delete|keepInternalOnly)")
	datasetIngestorCmd.Flags().Bool("allowexistingsource", false, "Defines if existing sourceFolders can be reused")
	datasetIngestorCmd.Flags().String("addattachment", "", "Filename of image to attach (single dataset case only)")
	datasetIngestorCmd.Flags().String("addcaption", "", "Optional caption to be stored with attachment (single dataset case only)")
	datasetIngestorCmd.Flags().String("globus-cfg", "", "Override globus transfer config file location [default: globus.yaml next to executable]")

	datasetIngestorCmd.MarkFlagsMutuallyExclusive("testenv", "devenv", "localenv", "tunnelenv")
	datasetIngestorCmd.MarkFlagsMutuallyExclusive("nocopy", "copy")
}

func createLocalSymlinkCallbackForFileLister(fileService *backend.FileService, skipSymlinks *string) func(string, string) (bool, error) {
	scanner := bufio.NewScanner(os.Stdin)

	return func(symlinkPath string, sourceFolder string) (bool, error) {
		cliPromptHandler := func(warningMsg string, choicePrompt string) string {
			color.Set(color.FgYellow)
			log.Println(warningMsg)
			color.Unset()

			log.Print(choicePrompt)
			scanner.Scan()
			return scanner.Text()
		}

		keep, updatedStrategy := fileService.EvaluateSymlink(symlinkPath, sourceFolder, *skipSymlinks, cliPromptHandler)
		*skipSymlinks = updatedStrategy

		return keep, nil
	}
}
