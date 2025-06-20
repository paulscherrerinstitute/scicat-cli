package cmd

import (
	"bufio"
	"crypto/tls"
	"encoding/json"
	"fmt"
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

var datasetIngestorCmd = &cobra.Command{
	Use:   "datasetIngestor",
	Short: "Define and add a dataset to the SciCat datacatalog",
	Long: `Purpose: define and add a dataset to the SciCat datacatalog
	
This command must be run on the machine having access to the data 
which comprises the dataset. It takes one or two input 
files and creates the necessary messages which trigger 
the creation of the corresponding datacatalog entries

For further help see "` + MANUAL + `"

Special hints for the decentral use case, where data is copied first to intermediate storage:
For Linux you need to have a valid Kerberos tickets, which you can get via the kinit command.
For Windows you need instead to specify -user username:password on the command line.`,
	Args: rangeArgsWithVersionException(1, 2),
	Run: func(cmd *cobra.Command, args []string) {
		var tooLargeDatasets = 0
		var emptyDatasets = 0

		var originalMap = make(map[string]string)

		var client = &http.Client{
			Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: false}},
			Timeout:   120 * time.Second}

		// const PROD_RSYNC_ARCHIVE_SERVER string = "ebarema2in.psi.ch"
		// const TEST_RSYNC_ARCHIVE_SERVER string = "ebaremat1in.psi.ch"
		// const DEV_RSYNC_ARCHIVE_SERVER string = "arematest2in.psi.ch"

		const CMD = "datasetIngestor"

		var scanner = bufio.NewScanner(os.Stdin)

		var APIServer string = PROD_API_SERVER
		var RSYNCServer string = PROD_RSYNC_ARCHIVE_SERVER
		var env string = "production"

		// pass parameters
		ingestFlag, _ := cmd.Flags().GetBool("ingest")
		testenvFlag, _ := cmd.Flags().GetBool("testenv")
		devenvFlag, _ := cmd.Flags().GetBool("devenv")
		localenvFlag, _ := cmd.Flags().GetBool("localenv")
		tunnelenvFlag, _ := cmd.Flags().GetBool("tunnelenv")
		scicatUrl, _ := cmd.Flags().GetString("scicat-url")
		rsyncUrl, _ := cmd.Flags().GetString("rsync-url")
		noninteractiveFlag, _ := cmd.Flags().GetBool("noninteractive")
		userpass, _ := cmd.Flags().GetString("user")
		token, _ := cmd.Flags().GetString("token")
		oidc, _ := cmd.Flags().GetBool("oidc")
		copyFlag, _ := cmd.Flags().GetBool("copy")
		nocopyFlag, _ := cmd.Flags().GetBool("nocopy")
		transferTypeFlag, _ := cmd.Flags().GetString("transfer-type")
		tapecopies, _ := cmd.Flags().GetInt("tapecopies")
		autoarchiveFlag, _ := cmd.Flags().GetBool("autoarchive")
		linkfiles, _ := cmd.Flags().GetString("linkfiles")
		allowExistingSourceFolder, _ := cmd.Flags().GetBool("allowexistingsource")
		addAttachment, _ := cmd.Flags().GetString("addattachment")
		addCaption, _ := cmd.Flags().GetString("addcaption")
		showVersion, _ := cmd.Flags().GetBool("version")
		globusCfgFlag, _ := cmd.Flags().GetString("globus-cfg")

		// TODO: read in CFG!

		// transfer type
		transferType, err := cliutils.ConvertToTransferType(transferTypeFlag)
		if err != nil {
			log.Fatalln(err)
		}

		var transferFiles func(params cliutils.TransferParams) (archivable bool, err error)

		// globus specific vars (if needed)
		var globusClient globus.GlobusClient
		var gConfig cliutils.GlobusConfig

		switch transferType {
		case cliutils.Ssh:
			transferFiles = cliutils.SshTransfer
		case cliutils.Globus:
			transferFiles = cliutils.GlobusTransfer
			var globusConfigPath string
			if cmd.Flags().Lookup("globus-cfg").Changed {
				globusConfigPath = globusCfgFlag
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

			if autoarchiveFlag {
				log.Fatalln("Cannot autoarchive when transferring via Globus due to the transfer happening asynchronously. Use the \"globusCheckTransfer\" command to archive them")
			}
		}

		if datasetUtils.TestFlags != nil {
			datasetUtils.TestFlags(map[string]interface{}{
				"ingest":              ingestFlag,
				"testenv":             testenvFlag,
				"devenv":              devenvFlag,
				"localenv":            localenvFlag,
				"tunnelenv":           tunnelenvFlag,
				"scicat-url":          scicatUrl,
				"rsync-url":           rsyncUrl,
				"noninteractive":      noninteractiveFlag,
				"user":                userpass,
				"token":               token,
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

		// === check for program version ===
		datasetUtils.CheckForNewVersion(client, CMD, VERSION)
		datasetUtils.CheckForServiceAvailability(client, testenvFlag, autoarchiveFlag)

		// environment overrides
		if tunnelenvFlag {
			APIServer = TUNNEL_API_SERVER
			RSYNCServer = TUNNEL_RSYNC_ARCHIVE_SERVER
			env = "dev"
		}
		if localenvFlag {
			APIServer = LOCAL_API_SERVER
			RSYNCServer = LOCAL_RSYNC_ARCHIVE_SERVER
			env = "local"
		}
		if devenvFlag {
			APIServer = DEV_API_SERVER
			RSYNCServer = DEV_RSYNC_ARCHIVE_SERVER
			env = "dev"
		}
		if testenvFlag {
			APIServer = TEST_API_SERVER
			RSYNCServer = TEST_RSYNC_ARCHIVE_SERVER
			env = "test"
		}
		if scicatUrl != "" {
			APIServer = scicatUrl
			if rsyncUrl != "" {
				RSYNCServer = rsyncUrl
				env = "custom"
			} else {
				env = "custom-" + env
			}
		}

		color.Set(color.FgGreen)
		log.Printf("You are about to add a dataset to the === %s === data catalog environment...", env)
		color.Unset()

		user, accessGroups, err := authenticate(RealAuthenticator{}, client, APIServer, userpass, token, oidc)
		if err != nil {
			log.Fatal(err)
		}

		/* TODO Add info about policy settings and that autoarchive will take place or not */
		metaDataMap, metadataSourceFolder, beamlineAccount, err := datasetIngestor.ReadAndCheckMetadata(client, APIServer, metadatafile, user, accessGroups)
		if err != nil {
			log.Fatal("Error in CheckMetadata function: ", err)
		}
		//log.Printf("metadata object: %v\n", metaDataMap)

		// assemble list of datasetPaths (=datasets) to be created
		var datasetPaths []string
		if folderListingTxt == "" {
			datasetPaths = append(datasetPaths, metadataSourceFolder)
		} else {
			// get folders from file
			folderlist, err := os.ReadFile(folderListingTxt)
			if err != nil {
				log.Fatal(err)
			}
			lines := strings.Split(string(folderlist), "\n")
			// remove all empty and comment lines
			for _, line := range lines {
				if line == "" || string(line[0]) == "#" {
					continue
				}
				// NOTE what is this special third level "data" folder that needs to be unsymlinked?
				// convert into canonical form only for certain online data linked from eaccounts home directories
				var parts = strings.Split(line, "/")
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
		}
		// log.Printf("Selected folders: %v\n", folders)

		// test if a sourceFolder already used in the past and give warning
		log.Println("Testing for existing source folders...")
		foundList, err := datasetIngestor.TestForExistingSourceFolder(datasetPaths, client, APIServer, user["accessToken"])
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
		if !allowExistingSourceFolder && len(foundList) > 0 {
			if !cmd.Flags().Changed("allowexistingsource") {
				log.Printf("Do you want to ingest the corresponding new datasets nevertheless (y/N) ? ")
				scanner.Scan()
				archiveAgain := scanner.Text()
				if archiveAgain != "y" {
					log.Fatalln("Aborted.")
				}
			} else {
				log.Fatalln("Existing sourceFolders are not allowed. Aborted.")
			}
		}

		// TODO ask archive system if sourcefolder is known to them. If yes no copy needed, otherwise
		// a destination location is defined by the archive system
		// for now let the user decide if he needs a copy

		if nocopyFlag {
			copyFlag = false
		}
		checkCentralAvailability := !(cmd.Flags().Changed("copy") || cmd.Flags().Changed("nocopy") || beamlineAccount || copyFlag)
		skipSymlinks := ""

		// check if skip flag is globally defined via flags:
		if cmd.Flags().Changed("linkfiles") {
			switch linkfiles {
			case "delete":
				skipSymlinks = "sA"
			case "keep":
				skipSymlinks = "kA"
			default:
				skipSymlinks = "dA" // default behaviour = keep internal for all
			}
		}

		var skippedLinks uint = 0
		var illegalFileNames uint = 0
		localSymlinkCallback := createLocalSymlinkCallbackForFileLister(&skipSymlinks, &skippedLinks)
		localFilepathFilterCallback := createLocalFilenameFilterCallback(&illegalFileNames)

		// now everything is prepared, prepare to loop over all folders
		var archivableDatasetList []string
		archivableDatasetListOwnerGroup, ok := metaDataMap["ownerGroup"].(string)
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
			if !(skipSymlinks == "sA" || skipSymlinks == "kA" || skipSymlinks == "dA") {
				skipSymlinks = ""
			}

			// === get filelist of dataset ===
			log.Printf("Getting filelist for \"%s\"...\n", datasetSourceFolder)
			fullFileArray, startTime, endTime, owner, numFiles, totalSize, err :=
				datasetIngestor.GetLocalFileList(datasetSourceFolder, datasetFileListTxt, localSymlinkCallback, localFilepathFilterCallback)
			if err != nil {
				log.Fatalf("Can't gather the filelist of \"%s\"", datasetSourceFolder)
			}
			log.Println("File list collected.")
			//log.Printf("full fileListing: %v\n Start and end time: %s %s\n ", fullFileArray, startTime, endTime)
			log.Printf("The dataset contains %v files with a total size of %v bytes.\n", numFiles, totalSize)

			// filecount checks
			if totalSize == 0 {
				emptyDatasets++
				color.Set(color.FgRed)
				log.Printf("\"%s\" dataset cannot be ingested - contains no files\n", datasetSourceFolder)
				color.Unset()
				continue
			}
			if numFiles > cliutils.TOTAL_MAXFILES {
				tooLargeDatasets++
				color.Set(color.FgRed)
				log.Printf("\"%s\" dataset cannot be ingested - too many files: has %d, max. %d\n", datasetSourceFolder, numFiles, cliutils.TOTAL_MAXFILES)
				color.Unset()
				continue
			}

			// NOTE: only tapecopies=1 or 2 does something if set.
			if tapecopies == 2 {
				color.Set(color.FgYellow)
				log.Printf("Note: this dataset, if archived, will be copied to two tape copies")
				color.Unset()
			}
			// === update metadata ===
			datasetIngestor.UpdateMetaData(client, APIServer, user, originalMap, metaDataMap, startTime, endTime, owner, tapecopies)
			pretty, _ := json.MarshalIndent(metaDataMap, "", "    ")

			log.Printf("Updated metadata object:\n%s\n", pretty)

			// === check central availability of data ===
			// check if data is accesible at archive server, unless beamline account (assumed to be centrally available always)
			// and unless (no)copy flag defined via command line
			if checkCentralAvailability {
				log.Println("Checking if data is centrally available...")
				sshErr, otherErr := datasetIngestor.CheckDataCentrallyAvailableSsh(user["username"], RSYNCServer, datasetSourceFolder, os.Stdout)
				if otherErr != nil {
					log.Fatalln("Cannot check if data is centrally available:", otherErr)
				}
				// if the ssh command's error is not nil, the dataset is *likely* to be not centrally available (maybe should check the error returned)
				if sshErr != nil {
					color.Set(color.FgYellow)
					log.Printf("The source folder %v is not centrally available.\nThe data must first be copied.\n ", datasetSourceFolder)
					color.Unset()
					copyFlag = true
					// check if user account
					if len(accessGroups) == 0 {
						color.Set(color.FgRed)
						log.Println("For copying, you must use a personal account. Beamline accounts are not supported.")
						color.Unset()
						os.Exit(1)
					}
					if !noninteractiveFlag {
						log.Printf("Do you want to continue (Y/n)? ")
						scanner.Scan()
						continueFlag := scanner.Text()
						if continueFlag == "n" {
							log.Fatalln("Further ingests interrupted because copying is needed, but no copy wanted.")
						}
					}
				} else {
					log.Println("Data is present centrally.")
				}
			}

			// === ingest dataset ===
			if ingestFlag {
				// create ingest . For decentral case delay setting status to archivable until data is copied
				archivable := false
				if _, ok := metaDataMap["datasetlifecycle"]; !ok {
					metaDataMap["datasetlifecycle"] = map[string]interface{}{}
				}
				if copyFlag { // IDEA: maybe add a flag to indicate that we want to copy later?
					// do not override existing fields
					metaDataMap["datasetlifecycle"].(map[string]interface{})["isOnCentralDisk"] = false
					metaDataMap["datasetlifecycle"].(map[string]interface{})["archiveStatusMessage"] = "filesNotYetAvailable"
					metaDataMap["datasetlifecycle"].(map[string]interface{})["archivable"] = false
				} else {
					archivable = true
					metaDataMap["datasetlifecycle"].(map[string]interface{})["isOnCentralDisk"] = true
					metaDataMap["datasetlifecycle"].(map[string]interface{})["archiveStatusMessage"] = "datasetCreated"
					metaDataMap["datasetlifecycle"].(map[string]interface{})["archivable"] = true
				}
				log.Println("Ingesting dataset...")
				datasetId, err := datasetIngestor.IngestDataset(client, APIServer, metaDataMap, fullFileArray, user)
				if err != nil {
					log.Fatal("Couldn't ingest dataset:", err)
				}
				log.Println("Dataset created:", datasetId)
				// add attachment optionally
				if addAttachment != "" {
					log.Println("Adding attachment...")
					err := datasetIngestor.AddAttachment(client, APIServer, datasetId, metaDataMap, user["accessToken"], addAttachment, addCaption)
					if err != nil {
						log.Println("Couldn't add attachment:", err)
					}
					log.Printf("Attachment file %v added to dataset %v\n", addAttachment, datasetId)
				}
				// === copying files ===
				if copyFlag {
					var err error = nil
					// convert fullFileArray to a list of paths and symlink tests
					var filePathList []string
					var isSymlinkList []bool
					for _, file := range fullFileArray {
						filePathList = append(filePathList, file.Path)
						isSymlinkList = append(isSymlinkList, file.IsSymlink)
					}
					params := cliutils.TransferParams{
						SshParams: cliutils.SshParams{
							Client:          client,
							User:            user,
							ApiServer:       APIServer,
							RsyncServer:     RSYNCServer,
							AbsFilelistPath: absFileListing,
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

					archivable, err = transferFiles(params)
					if err != nil {
						color.Set(color.FgRed)
						log.Printf("The  command to copy files exited with error %v \n", err)
						log.Printf("The dataset %v is not yet in an archivable state\n", datasetId)
						color.Unset()
					}
					if err == nil && !archivable {
						color.Set(color.FgYellow)
						log.Println("The command finished successfully, however the dataset is not yet archivable.")
						log.Println("This means that the dataset has to be marked as archivable after the asynchronous transfer has finished.")
						log.Printf("Please consult the %s transfer type's doc for handling this.\n", transferTypeFlag)
						color.Unset()
					}
				}

				if archivable {
					archivableDatasetList = append(archivableDatasetList, datasetId)
				}
			}
			// reset dataset metadata for next dataset ingestion
			datasetIngestor.ResetUpdatedMetaData(originalMap, metaDataMap)
		}

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
		if skippedLinks > 0 {
			color.Set(color.FgYellow)
			log.Printf("Total number of link files skipped:%v\n", skippedLinks)
		}
		if illegalFileNames > 0 {
			color.Set(color.FgRed)
			log.Printf("Number of files ignored because of illegal filenames:%v\n", illegalFileNames)
		}
		color.Unset()

		// stop here if empty datasets appeared
		if emptyDatasets > 0 || tooLargeDatasets > 0 {
			os.Exit(1)
		}

		// === create archive jobs ===
		if autoarchiveFlag && ingestFlag {
			log.Printf("Submitting Archive Job for the ingested datasets.\n")
			// TODO: change param type from pointer to regular as it is unnecessary
			//   for it to be passed as pointer
			jobId, err := datasetUtils.CreateArchivalJob(client, APIServer, user, archivableDatasetListOwnerGroup, archivableDatasetList, &tapecopies, nil)

			if err != nil {
				color.Set(color.FgRed)
				log.Printf("Could not create the archival job for the ingested datasets: %s\n", err.Error())
				color.Unset()
			}

			log.Println("Submitted job:", jobId)
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

func createLocalSymlinkCallbackForFileLister(skipSymlinks *string, skippedLinks *uint) func(symlinkPath string, sourceFolder string) (bool, error) {
	scanner := bufio.NewScanner(os.Stdin)
	return func(symlinkPath string, sourceFolder string) (bool, error) {
		keep := true
		pointee, _ := os.Readlink(symlinkPath) // just pass the file name
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
				log.Printf("keep variable set to %v", keep)
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

func createLocalFilenameFilterCallback(illegalFileNamesCounter *uint) func(filepath string) bool {
	return func(filepath string) (keep bool) {
		keep = true
		// make sure that filenames do not contain characters like "\" or "*"
		if strings.ContainsAny(filepath, "*\\") {
			color.Set(color.FgRed)
			log.Printf("Warning: the file %s contains illegal characters like *,\\ and will not be archived.", filepath)
			color.Unset()
			if illegalFileNamesCounter != nil {
				*illegalFileNamesCounter++
			}
			keep = false
		}
		// and check for triple blanks, they are used to separate columns in messages
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
