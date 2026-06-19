package cmd

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/fatih/color"
	"github.com/paulscherrerinstitute/scicat-cli/v3/cmd/cliutils"
	"github.com/paulscherrerinstitute/scicat-cli/v3/datasetIngestor"
	"github.com/paulscherrerinstitute/scicat-cli/v3/datasetUtils"
	"github.com/paulscherrerinstitute/scicat-cli/v3/internal/backend"
	"github.com/spf13/cobra"
)

var datasetIngestorCmd = &cobra.Command{
	Use:   "datasetIngestor",
	Short: "Define and add a dataset to the SciCat datacatalog",
	Long: `Purpose: define and add a dataset to the SciCat datacatalog

This command must be run on the machine having access to the data
which comprises the dataset. It takes one or two input
files and creates the necessary messages which trigger
the creation of the corresponding datacatalog entries

For further help see "` + cliutils.MANUAL + `"

Special hints for the decentral use case, where data is copied first to intermediate storage:
For Linux you need to have a valid Kerberos tickets, which you can get via the kinit command.
For Windows you need instead to specify -user username:password on the command line.`,
	Args: rangeArgsWithVersionException(1, 2),
	Run: func(cmd *cobra.Command, args []string) {
		var tooLargeDatasets = 0
		var emptyDatasets = 0

		var originalMap = make(map[string]string)

		const CMD = "datasetIngestor"

		var scanner = bufio.NewScanner(os.Stdin)

		envConfig := cliutils.InputEnvironmentConfig{
			TestenvFlag:   cliutils.GetCobraBoolFlag(cmd, "testenv"),
			DevenvFlag:    cliutils.GetCobraBoolFlag(cmd, "devenv"),
			TunnelenvFlag: cliutils.GetCobraBoolFlag(cmd, "tunnelenv"),
			LocalenvFlag:  cliutils.GetCobraBoolFlag(cmd, "localenv"),
			ScicatUrl:     cliutils.GetCobraStringFlag(cmd, "scicat-url"),
			RsyncUrl:      cliutils.GetCobraStringFlag(cmd, "rsync-url"),
		}

		authOpts := backend.AuthOptions{
			User:        cliutils.GetCobraStringFlag(cmd, "user"),
			Token:       cliutils.GetCobraStringFlag(cmd, "token"),
			Oidc:        cliutils.GetCobraBoolFlag(cmd, "oidc"),
			TestEnv:     envConfig.TestenvFlag,
			AutoArchive: cliutils.GetCobraBoolFlag(cmd, "autoarchive"),
		}

		transportEngine := backend.BootstrapTransportEngine(
			envConfig.ResolveAPIServer(),
			envConfig.ResolveRSYNCServer(),
		)

		userSession, err := transportEngine.InitializeSession(VERSION, authOpts)
		if err != nil {
			log.Fatalf("Initialization failed: %v", err)
		}

		var client = transportEngine.Client
		var APIServer = transportEngine.APIServer
		var RSYNCServer = transportEngine.RsyncServer
		var user = userSession.User
		var accessGroups = userSession.AccessGroups

		ingestFlag := cliutils.GetCobraBoolFlag(cmd, "ingest")
		noninteractiveFlag := cliutils.GetCobraBoolFlag(cmd, "noninteractive")
		copyFlag := cliutils.GetCobraBoolFlag(cmd, "copy")
		nocopyFlag := cliutils.GetCobraBoolFlag(cmd, "nocopy")
		transferTypeFlag := cliutils.GetCobraStringFlag(cmd, "transfer-type")
		tapecopies := cliutils.GetCobraIntFlag(cmd, "tapecopies")
		autoarchiveFlag := cliutils.GetCobraBoolFlag(cmd, "autoarchive")
		linkfiles := cliutils.GetCobraStringFlag(cmd, "linkfiles")
		allowExistingSourceFolder := cliutils.GetCobraBoolFlag(cmd, "allowexistingsource")
		addAttachment := cliutils.GetCobraStringFlag(cmd, "addattachment")
		addCaption := cliutils.GetCobraStringFlag(cmd, "addcaption")
		showVersion := cliutils.GetCobraBoolFlag(cmd, "version")
		globusCfgFlag := cliutils.GetCobraStringFlag(cmd, "globus-cfg")

		// TODO: read in CFG!

		if datasetUtils.TestFlags != nil {
			datasetUtils.TestFlags(map[string]interface{}{
				"ingest":              ingestFlag,
				"scicat-url":          APIServer,
				"rsync-url":           RSYNCServer,
				"noninteractive":      noninteractiveFlag,
				"user":                authOpts.User,
				"token":               authOpts.Token,
				"copy":                copyFlag,
				"nocopy":              nocopyFlag,
				"tapecopies":          tapecopies,
				"autoarchive":         autoarchiveFlag,
				"linkfiles":           linkfiles,
				"allowexistingsource": allowExistingSourceFolder,
				"addattachment":       addAttachment,
				"addcaption":          addCaption,
				"version":             showVersion,
			})
			return
		}

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
				// NOTE folderListingTxt is a TEXT FILE that lists dataset folders that should all be ingested together
				//   WITH the same metadata EXCEPT for the sourceFolder path (which is set during ingestion)
				folderListingTxt = args[1]
			} else {
				// NOTE datasetFileListTxt is a TEXT FILE that lists the files & folders of a dataset (contained in a folder)
				//   that should be considered as "part of" the dataset. The paths must be relative to the sourceFolder.
				datasetFileListTxt = args[1]
				absFileListing, _ = filepath.Abs(datasetFileListTxt)
			}
		}

		if datasetUtils.TestArgs != nil {
			datasetUtils.TestArgs([]interface{}{metadatafile, datasetFileListTxt, folderListingTxt})
			return
		}

		if showVersion {
			fmt.Printf("%s\n", VERSION)
			return
		}

		/* TODO Add info about policy settings and that autoarchive will take place or not */
		metaDataMap, metadataSourceFolder, beamlineAccount, err := datasetIngestor.ReadAndCheckMetadata(client, APIServer, metadatafile, user, accessGroups)
		if err != nil {
			log.Fatal("Error in CheckMetadata function: ", err)
		}

		fileService := backend.NewFileService(user, metadataSourceFolder, folderListingTxt, absFileListing, RSYNCServer)
		err = fileService.InitializeTransferStrategy(
			transferTypeFlag,
			globusCfgFlag,
			cmd.Flags().Lookup("globus-cfg").Changed,
			autoarchiveFlag,
		)
		if err != nil {
			log.Fatalln("Strategy allocation failed:", err)
		}

		ingestService := backend.NewIngestService(transportEngine, fileService)

		batch, err := ingestService.PrepareBatch(metadatafile, absFileListing)
		if err != nil {
			log.Fatal(err)
		}

		// assemble list of datasetPaths (=datasets) to be created
		datasetPaths := fileService.ResolveDatasetPaths()

		// test if a sourceFolder already used in the past and give warning
		log.Println("Testing for existing source folders...")
		foundList, err := ingestService.CheckExistingSources(datasetPaths)
		if err != nil {
			log.Fatal(err)
		}
		if len(foundList) > 0 {
			color.Set(color.FgYellow)
			fmt.Println("Warning! The following datasets have been found with the same sourceFolders:")
			for _, element := range foundList {
				fmt.Printf("  - PID: \"%s\", sourceFolder: \"%s\"\n", element.Pid, element.SourceFolder)
			}
			color.Unset()

			// Evaluate flag logic and user intent locally
			if !allowExistingSourceFolder && cmd.Flags().Changed("allowexistingsource") {
				log.Fatalln("Existing sourceFolders are not allowed. Aborted.")
			}
			if !allowExistingSourceFolder && !cmd.Flags().Changed("allowexistingsource") {
				log.Printf("Do you want to continue (y/N) ? ")
				scanner.Scan()
				if scanner.Text() != "y" {
					log.Fatalln("Aborted.")
				}
			}
		} else {
			log.Println("Finished testing for existing source folders.")
		}
		// TODO ask archive system if sourcefolder is known to them. If yes no copy needed, otherwise
		// a destination location is defined by the archive system
		// for now let the user decide if he needs a copy

		if nocopyFlag {
			copyFlag = false
		}
		checkCentralAvailability := !(cmd.Flags().Changed("copy") || cmd.Flags().Changed("nocopy") || beamlineAccount || copyFlag)
		skipSymlinks := fileService.EvaluateSymlinkStrategy(cmd.Flags().Changed("linkfiles"), linkfiles)
		runtimeCfg := backend.DatasetIngestRuntimeConfig{
			Tapecopies:    tapecopies,
			AddAttachment: addAttachment,
			AddCaption:    addCaption,
			RSYNCServer:   transportEngine.RsyncServer,
		}
		archivableDatasetListOwnerGroup := batch.MetaDataMap["ownerGroup"].(string)

		// now everything is prepared, prepare to loop over all folders
		var archivableDatasetList []string
		archivableDatasetListOwnerGroup, ok := batch.MetaDataMap["ownerGroup"].(string)
		if !ok {
			log.Fatal("can't recover ownerGroup. This should normally be impossible as the checkMetadata function should've caught it already.")
		}
		for _, datasetSourceFolder := range datasetPaths {
			log.Printf("===== Ingesting: \"%s\" =====\n", datasetSourceFolder)
			// ignore empty lines
			if datasetSourceFolder == "" {
				// NOTE if there are empty source folder(s), shouldn't we raise an error?
				continue
			}
			metaDataMap["sourceFolder"] = datasetSourceFolder
			log.Printf("Scanning files in dataset %s", datasetSourceFolder)

			// reset skip var. if not set for all datasets
			skipSymlinks = fileService.ResetLocalSymlinkStrategy(skipSymlinks)
			localSymlinkCallback := createLocalSymlinkCallbackForFileLister(fileService, &skipSymlinks)

			// === get filelist of dataset ===
			fullFileArray, startTime, endTime, owner, isValid, err := fileService.ScanAndVerifyFiles(
				datasetSourceFolder,
				datasetFileListTxt,
				localSymlinkCallback,
			)
			if err != nil {
				log.Fatalf("Verification failed for \"%s\": %v", datasetSourceFolder, err)
			}
			log.Println("File list collected.")

			if !isValid {
				continue
			}

			// NOTE: only tapecopies=1 or 2 does something if set.
			if tapecopies == 2 {
				color.Set(color.FgYellow)
				log.Printf("Note: this dataset, if archived, will be copied to two tape copies")
				color.Unset()
			}

			// === check central availability of data ===
			// check if data is accesible at archive server, unless beamline account (assumed to be centrally available always)
			// and unless (no)copy flag defined via command line
			if checkCentralAvailability {
				log.Println("Checking if data is centrally available...")
				needsCopy, sshErr := fileService.AuditCentralDataAvailability(datasetSourceFolder)
				if sshErr != nil && needsCopy == false {
					log.Fatalln("Cannot check if data is centrally available:", sshErr)
				}
				// if the ssh command's error is not nil, the dataset is *likely* to be not centrally available (maybe should check the error returned)
				if needsCopy {
					color.Set(color.FgYellow)
					log.Printf("The source folder %v is not centrally available.\nThe data must first be copied.\n ", datasetSourceFolder)
					color.Unset()

					copyFlag = true

					if len(accessGroups) == 0 {
						color.Set(color.FgRed)
						log.Println("For copying, you must use a personal account. Beamline accounts are not supported.")
						color.Unset()
						os.Exit(1)
					}

					if !noninteractiveFlag {
						log.Printf("Do you want to continue (Y/n)? ")
						scanner.Scan()
						if scanner.Text() == "n" {
							log.Fatalln("Further ingests interrupted because copying is needed, but no copy wanted.")
						}
					}
				}
			}

			if ingestFlag {
				_, err := ingestService.Ingest(batch, datasetSourceFolder, fullFileArray, startTime, endTime, owner, copyFlag, runtimeCfg)

				if err != nil {
					log.Fatalf("Ingestion sequence aborted: %v", err)
				}
			}
			// reset dataset metadata for next dataset ingestion
			datasetIngestor.ResetUpdatedMetaData(originalMap, metaDataMap)
		}

		if fileService.EmptyDatasetsCount > 0 {
			color.Set(color.FgRed)
			log.Printf("Total empty datasets skipped: %d\n", fileService.EmptyDatasetsCount)
		}
		if fileService.TooLargeDatasetsCount > 0 {
			color.Set(color.FgRed)
			log.Printf("Total oversized datasets skipped: %d\n", fileService.TooLargeDatasetsCount)
		}
		color.Unset()

		if !ingestFlag {
			color.Set(color.FgRed)
			log.Printf("Note: you run in 'dry' mode to simply to check data consistency. Use the --ingest flag to really ingest datasets.")
		}

		if emptyDatasets > 0 {
			color.Set(color.FgRed)
			log.Printf("Number of datasets not stored because they are empty:%v\n. Please note that this will cancel any subsequent archive steps from this job !\n", emptyDatasets)
		}
		if tooLargeDatasets > 0 {
			color.Set(color.FgRed)
			log.Printf("Number of datasets not stored because of too many files:%v\nPlease note that this will cancel any subsequent archive steps from this job !\n", tooLargeDatasets)
		}
		color.Unset()
		// print file statistics
		if fileService.TotalSkippedLinks > 0 {
			color.Set(color.FgYellow)
			log.Printf("Total number of link files skipped:%v\n", fileService.TotalSkippedLinks)
		}
		if fileService.TotalIllegalFileNames > 0 {
			color.Set(color.FgRed)
			log.Printf("Number of files ignored because of illegal filenames:%v\n", fileService.TotalIllegalFileNames)
		}
		color.Unset()

		// stop here if empty datasets appeared
		if emptyDatasets > 0 || tooLargeDatasets > 0 {
			os.Exit(1)
		}

		// === create archive jobs ===
		if autoarchiveFlag && ingestFlag {
			archiveService := backend.NewArchiveService(transportEngine)
			_, err := archiveService.SubmitArchivalJob(archivableDatasetListOwnerGroup, archivableDatasetList, tapecopies)
			if err != nil {
				color.Set(color.FgRed)
				log.Printf("Could not create the archival job for the ingested datasets: %s\n", err.Error())
				color.Unset()
			}
		}

		// print out results to STDOUT, one line per dataset
		for i := 0; i < len(archivableDatasetList); i++ {
			fmt.Println(archivableDatasetList[i])
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
	datasetIngestorCmd.Flags().Bool("copy", false, "Defines if files should be copied from your local system to a central server before ingest (i.e. your data is not centrally available and therefore needs to be copied ='decentral' case). copyFlag has higher priority than nocopyFlag. If neither flag is defined the tool will try to make the best guess.")
	datasetIngestorCmd.Flags().Bool("nocopy", false, "Defines if files should *not* be copied from your local system to a central server before ingest (i.e. your data is centrally available and therefore does not need to be copied ='central' case).")
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

		if keep {
			color.Set(color.FgGreen)
		} else {
			color.Set(color.FgRed)
		}
		color.Unset()

		return keep, nil
	}
}
