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

	"github.com/fatih/color"
	"github.com/paulscherrerinstitute/scicat/datasetIngestor"
	"github.com/paulscherrerinstitute/scicat/datasetUtils"
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

		const TOTAL_MAXFILES = 400000

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
		noninteractiveFlag, _ := cmd.Flags().GetBool("noninteractive")
		userpass, _ := cmd.Flags().GetString("user")
		token, _ := cmd.Flags().GetString("token")
		copyFlag, _ := cmd.Flags().GetBool("copy")
		nocopyFlag, _ := cmd.Flags().GetBool("nocopy") // TODO why is there even a "no copy" flag?
		tapecopies, _ := cmd.Flags().GetInt("tapecopies")
		autoarchiveFlag, _ := cmd.Flags().GetBool("autoarchive")
		linkfiles, _ := cmd.Flags().GetString("linkfiles")
		allowExistingSourceFolder, _ := cmd.Flags().GetBool("allowexistingsource")
		addAttachment, _ := cmd.Flags().GetString("addattachment")
		addCaption, _ := cmd.Flags().GetString("addcaption")
		showVersion, _ := cmd.Flags().GetBool("version")

		if datasetUtils.TestFlags != nil {
			datasetUtils.TestFlags(map[string]interface{}{
				"ingest":              ingestFlag,
				"testenv":             testenvFlag,
				"devenv":              devenvFlag,
				"localenv":            localenvFlag,
				"tunnelenv":           tunnelenvFlag,
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

		// check for program version
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

		color.Set(color.FgGreen)
		log.Printf("You are about to add a dataset to the === %s === data catalog environment...", env)
		color.Unset()

		user, accessGroups := authenticate(RealAuthenticator{}, client, APIServer, userpass, token)

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
			fmt.Println("Warning! The following datasets have been found with the same sourceFolder: ")
		}
		for _, element := range foundList {
			fmt.Printf("  - PID: \"%s\", sourceFolder: \"%s\"\n", element.Pid, element.SourceFolder)
		}
		color.Unset()
		if !allowExistingSourceFolder && len(foundList) > 0 {
			if cmd.Flags().Changed("allowexistingsource") {
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

		// now everything is prepared, prepare to loop over all folders
		if nocopyFlag {
			copyFlag = false
		}
		checkCentralAvailability := !(cmd.Flags().Changed("copy") || cmd.Flags().Changed("nocopy") || beamlineAccount || copyFlag)
		skip := ""

		// check if skip flag is globally defined via flags:
		if cmd.Flags().Changed("linkfiles") {
			switch linkfiles {
			case "delete":
				skip = "sA"
			case "keep":
				skip = "kA"
			default:
				skip = "dA" // default behaviour = keep internal for all
			}
		}

		var archivableDatasetList []string
		for _, datasetSourceFolder := range datasetPaths {
			log.Printf("===== Ingesting: \"%s\" =====\n", datasetSourceFolder)
			// ignore empty lines
			if datasetSourceFolder == "" {
				// NOTE if there are empty source folder(s), shouldn't we raise an error?
				continue
			}
			metaDataMap["sourceFolder"] = datasetSourceFolder
			log.Printf("Scanning files in dataset %s", datasetSourceFolder)

			// get filelist of dataset
			log.Printf("Getting filelist for \"%s\"...\n", datasetSourceFolder)
			fullFileArray, startTime, endTime, owner, numFiles, totalSize, err :=
				datasetIngestor.GetLocalFileList(datasetSourceFolder, datasetFileListTxt, &skip)
			if err != nil {
				log.Fatalf("Can't gather the filelist of \"%s\"", datasetSourceFolder)
			}
			log.Println("Filelist collected.")
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
			if numFiles > TOTAL_MAXFILES {
				tooLargeDatasets++
				color.Set(color.FgRed)
				log.Printf("\"%s\" dataset cannot be ingested - too many files: has %d, max. %d\n", datasetSourceFolder, numFiles, TOTAL_MAXFILES)
				color.Unset()
				continue
			}

			// NOTE: only tapecopies=1 or 2 does something if set.
			if tapecopies == 2 {
				color.Set(color.FgYellow)
				log.Printf("Note: this dataset, if archived, will be copied to two tape copies")
				color.Unset()
			}
			datasetIngestor.UpdateMetaData(client, APIServer, user, originalMap, metaDataMap, startTime, endTime, owner, tapecopies)
			pretty, _ := json.MarshalIndent(metaDataMap, "", "    ")

			log.Printf("Updated metadata object:\n%s\n", pretty)

			// check if data is accesible at archive server, unless beamline account (assumed to be centrally available always)
			// and unless (no)copy flag defined via command line
			if checkCentralAvailability {
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
				}
			}

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
					log.Printf("Attachment file %v added to dataset  %v\n", addAttachment, datasetId)
				}
				if copyFlag {
					// TODO rewrite SyncDataToFileserver
					log.Println("Syncing files to cache server...")
					err := datasetIngestor.SyncLocalDataToFileserver(datasetId, user, RSYNCServer, datasetSourceFolder, absFileListing, os.Stdout)
					if err == nil {
						// delayed enabling
						archivable = true
						err := datasetIngestor.MarkFilesReady(client, APIServer, datasetId, user)
						if err != nil {
							log.Fatal("Couldn't mark files ready:", err)
						}
					} else {
						color.Set(color.FgRed)
						log.Printf("The  command to copy files exited with error %v \n", err)
						log.Printf("The dataset %v is not yet in an archivable state\n", datasetId)
						// TODO let user decide to delete dataset entry
						// datasetIngestor.DeleteDatasetEntry(client, APIServer, datasetId, user["accessToken"])
						color.Unset()
					}
					log.Println("Syncing files - DONE")
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
		datasetIngestor.PrintFileInfos() // TODO: move this into cmd portion

		// stop here if empty datasets appeared
		if emptyDatasets > 0 || tooLargeDatasets > 0 {
			os.Exit(1)
		}
		// start archive job
		if autoarchiveFlag && ingestFlag {
			log.Printf("Submitting Archive Job for the ingested datasets.\n")
			// TODO: change param type from pointer to regular as it is unnecessary
			//   for it to be passed as pointer
			jobId, err := datasetUtils.CreateArchivalJob(client, APIServer, user, archivableDatasetList, &tapecopies)
			if err != nil {
				color.Set(color.FgRed)
				log.Printf("Could not create the archival job for the ingested datasets: %s", err.Error())
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
	datasetIngestorCmd.Flags().Bool("noninteractive", false, "If set no questions will be asked and the default settings for all undefined flags will be assumed")
	datasetIngestorCmd.Flags().Bool("copy", false, "Defines if files should be copied from your local system to a central server before ingest (i.e. your data is not centrally available and therefore needs to be copied ='decentral' case). copyFlag has higher priority than nocopyFlag. If neither flag is defined the tool will try to make the best guess.")
	datasetIngestorCmd.Flags().Bool("nocopy", false, "Defines if files should *not* be copied from your local system to a central server before ingest (i.e. your data is centrally available and therefore does not need to be copied ='central' case).")
	datasetIngestorCmd.Flags().Int("tapecopies", 0, "Number of tapecopies to be used for archiving")
	datasetIngestorCmd.Flags().Bool("autoarchive", false, "Option to create archive job automatically after ingestion")
	datasetIngestorCmd.Flags().String("linkfiles", "keepInternalOnly", "Define what to do with symbolic links: (keep|delete|keepInternalOnly)")
	datasetIngestorCmd.Flags().Bool("allowexistingsource", false, "Defines if existing sourceFolders can be reused")
	datasetIngestorCmd.Flags().String("addattachment", "", "Filename of image to attach (single dataset case only)")
	datasetIngestorCmd.Flags().String("addcaption", "", "Optional caption to be stored with attachment (single dataset case only)")

	datasetIngestorCmd.MarkFlagsMutuallyExclusive("testenv", "devenv", "localenv", "tunnelenv")
	datasetIngestorCmd.MarkFlagsMutuallyExclusive("nocopy", "copy")
}
