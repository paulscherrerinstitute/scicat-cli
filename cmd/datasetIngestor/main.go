/*

Purpose: define and add a dataset to the SciCat datacatalog

This script must be run on the machine having access to the data which comprises the dataset
It takes one or two input files and creates the necessary messages which trigger the creation
of the corresponding datacatalog entries

  Input files

  - metadata.json: the metadata going with the data. The structure of the meta data
    depends on the type of the dataset (raw, derived, base)
    It must have  a type and a sourceFolder field defined
  - either name of filelisting file: contains list of files and folders which belong to the dataset.
    In the simplest case this is just the path to a single file or folder
        (TODO optionally: add exclusion regexp for file not to be included)
      All paths are relative to the sourceFolder defined inside the metadata.json files
  - or "folderlisting.txt": (implies empty filelisting, i.e all files in folders): contains list of
	sourceFolders as absolute path names, for each a dataset is created with the metadata defined above
	and only the sourceFolder field being substituted

  Output:
   - Optionally a copy of the data on a central rsync server if data is not stored on central system
   - Entries in the data catalog created via the dacat API
       - a new (Raw/Derived)Dataset entry
	   - the origDataBlocks entries
	   - optionally a new job if autoarchive is requested

   - Return code (useful for wrapper scripts):
	 rc=0: command excuted correctly
	 rc=1: command exited with errors and needs to be repeated

	Note: the log.Fatal function calls os.exit(1) already

*/

package main

import (
	"bufio"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/paulscherrerinstitute/scicat/datasetIngestor"
	"github.com/paulscherrerinstitute/scicat/datasetUtils"

	"github.com/fatih/color"
)

const TOTAL_MAXFILES = 400000

func isFlagPassed(name string) bool {
	found := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == name {
			found = true
		}
	})
	return found
}

var VERSION string

func main() {
	var tooLargeDatasets = 0
	var emptyDatasets = 0

	var originalMap = make(map[string]string)

	var client = &http.Client{
		Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: false}},
		Timeout:   120 * time.Second}

	const PROD_API_SERVER string = "https://dacat.psi.ch/api/v3"
	const TEST_API_SERVER string = "https://dacat-qa.psi.ch/api/v3"
	const DEV_API_SERVER string = "https://dacat-development.psi.ch/api/v3"
	const LOCAL_API_SERVER string = "http://localhost:3000/api/v3"
	const TUNNEL_API_SERVER string = "https://dacat-development.psi.ch:5443/api/v3"

	const PROD_RSYNC_ARCHIVE_SERVER string = "pb-archive.psi.ch"
	const TEST_RSYNC_ARCHIVE_SERVER string = "pbt-archive.psi.ch"
	const DEV_RSYNC_ARCHIVE_SERVER string = "arematest2in.psi.ch"
	const LOCAL_RSYNC_ARCHIVE_SERVER string = "localhost"
	const TUNNEL_RSYNC_ARCHIVE_SERVER string = "arematest2in.psi.ch:2022"

	// const PROD_RSYNC_ARCHIVE_SERVER string = "ebarema2in.psi.ch"
	// const TEST_RSYNC_ARCHIVE_SERVER string = "ebaremat1in.psi.ch"
	// const DEV_RSYNC_ARCHIVE_SERVER string = "arematest2in.psi.ch"

	const MANUAL string = "http://melanie.gitpages.psi.ch/SciCatPages"
	const APP = "datasetIngestor"

	var scanner = bufio.NewScanner(os.Stdin)
	var APIServer string
	var RSYNCServer string
	var env string

	// pass parameters
	ingestFlag := flag.Bool("ingest", false, "Defines if this command is meant to actually ingest data")
	testenvFlag := flag.Bool("testenv", false, "Use test environment (qa) instead of production environment")
	devenvFlag := flag.Bool("devenv", false, "Use development environment instead of production environment (developers only)")
	localenvFlag := flag.Bool("localenv", false, "Use local environment instead of production environment (developers only)")
	tunnelenvFlag := flag.Bool("tunnelenv", false, "Use tunneled API server at port 5443 to access development instance (developers only)")
	noninteractiveFlag := flag.Bool("noninteractive", false, "If true, no questions will be asked and the default settings for all undefined flags will be assumed")
	userpass := flag.String("user", "", "Defines optional username:password string. This can be used both for access to the data catalog API and for access to the intermediate storage server for the decentral use case")
	token := flag.String("token", "", "Defines API token for access to the data catalog API. It is now mandatory for normal user accounts, but optional for functional accounts. It takes precedence over username/pw.")
	copyFlag := flag.Bool("copy", false, "Defines if files should be copied from your local system to a central server before ingest (i.e. your data is not centrally available and therefore needs to be copied ='decentral' case). copyFlag has higher priority than nocopyFlag. If neither flag is defined the tool will try to make the best guess.")
	nocopyFlag := flag.Bool("nocopy", false, "Defines if files should *not* be copied from your local system to a central server before ingest (i.e. your data is centrally available and therefore does not need to be copied ='central' case).")
	tapecopies := flag.Int("tapecopies", 0, "Number of tapecopies to be used for archiving")
	autoarchiveFlag := flag.Bool("autoarchive", false, "Option to create archive job automatically after ingestion")
	linkfiles := flag.String("linkfiles", "keepInternalOnly", "Define what to do with symbolic links: (keep|delete|keepInternalOnly)")
	allowExistingSourceFolder := flag.Bool("allowexistingsource", false, "Defines if existing sourceFolders can be reused")
	addAttachment := flag.String("addattachment", "", "Filename of image to attach (single dataset case only)")
	addCaption := flag.String("addcaption", "", "Optional caption to be stored with attachment (single dataset case only)")
	showVersion := flag.Bool("version", false, "Show version number and exit")

	flag.Parse()

	// to distinguish between defined and undefined flags needed if interactive questions askes
	if !*noninteractiveFlag {
		if !isFlagPassed("linkfiles") {
			linkfiles = nil
		}
		if !isFlagPassed("allowexistingsource") {
			allowExistingSourceFolder = nil
		}
	}

	if *showVersion {
		fmt.Printf("%s\n", VERSION)
		return
	}

	// check for program version only if running interactively
	datasetUtils.CheckForNewVersion(client, APP, VERSION)
	datasetUtils.CheckForServiceAvailability(client, *testenvFlag, *autoarchiveFlag)

	//}

	if *testenvFlag {
		APIServer = TEST_API_SERVER
		RSYNCServer = TEST_RSYNC_ARCHIVE_SERVER
		env = "test"
	} else if *devenvFlag {
		APIServer = DEV_API_SERVER
		RSYNCServer = DEV_RSYNC_ARCHIVE_SERVER
		env = "dev"
	} else if *localenvFlag {
		APIServer = LOCAL_API_SERVER
		RSYNCServer = LOCAL_RSYNC_ARCHIVE_SERVER
		env = "local"
	} else if *tunnelenvFlag {
		APIServer = TUNNEL_API_SERVER
		RSYNCServer = TUNNEL_RSYNC_ARCHIVE_SERVER
		env = "dev"
	} else {
		APIServer = PROD_API_SERVER
		RSYNCServer = PROD_RSYNC_ARCHIVE_SERVER
		env = "production"
	}

	color.Set(color.FgGreen)
	log.Printf("You are about to add a dataset to the === %s === data catalog environment...", env)
	color.Unset()

	args := flag.Args()
	metadatafile := ""
	filelistingPath := ""
	folderlistingPath := ""
	absFileListing := ""

	if len(args) == 1 {
		metadatafile = args[0]
	} else if len(args) == 2 {
		metadatafile = args[0]
		if args[1] == "folderlisting.txt" {
			folderlistingPath = args[1]
		} else {
			filelistingPath = args[1]
			absFileListing, _ = filepath.Abs(filelistingPath)
		}
	} else {
		fmt.Printf("\n\nTool to ingest datasets to the data catalog.\n\n")
		fmt.Printf("Run script with either 1 or 2 arguments:\n\n")
		fmt.Printf("datasetIngestor [options] metadata-file [filelisting-file|'folderlisting.txt']\n\n")
		flag.PrintDefaults()
		fmt.Printf("\n\nFor further help see " + MANUAL + "\n")
		fmt.Printf("\nSpecial hints for the decentral use case, where data is copied first to intermediate storage:\n")
		fmt.Printf("For Linux you need to have a valid Kerberos tickets, which you can get via the kinit command.\n")
		fmt.Printf("For Windows you need instead to specify -user username:password on the command line.\n")
		return
	}

	auth := &datasetUtils.RealAuthenticator{}
	user, accessGroups := datasetUtils.Authenticate(auth, client, APIServer, token, userpass)

	/* TODO Add info about policy settings and that autoarchive will take place or not */

	metaDataMap, sourceFolder, beamlineAccount, err := datasetIngestor.CheckMetadata(client, APIServer, metadatafile, user, accessGroups)
	if err != nil {
		log.Fatal("Error in CheckMetadata function: ", err)
	}
	//log.Printf("metadata object: %v\n", metaDataMap)

	// assemble list of folders (=datasets) to created
	var folders []string
	if folderlistingPath == "" {
		folders = append(folders, sourceFolder)
	} else {
		// get folders from file
		folderlist, err := os.ReadFile(folderlistingPath)
		if err != nil {
			log.Fatal(err)
		}
		lines := strings.Split(string(folderlist), "\n")
		// remove all empty and comment lines
		for _, sourceFolder := range lines {
			if sourceFolder != "" && string(sourceFolder[0]) != "#" {
				// convert into canonical form only for certain online data linked from eaccounts home directories
				var parts = strings.Split(sourceFolder, "/")
				if len(parts) > 3 && parts[3] == "data" {
					realSourceFolder, err := filepath.EvalSymlinks(sourceFolder)
					if err != nil {
						log.Fatalf("Failed to find canonical form of sourceFolder:%v %v", sourceFolder, err)
					}
					color.Set(color.FgYellow)
					log.Printf("Transform sourceFolder %v to canonical form: %v", sourceFolder, realSourceFolder)
					color.Unset()
					folders = append(folders, realSourceFolder)
				} else {
					folders = append(folders, sourceFolder)
				}
			}
		}
	}
	// log.Printf("Selected folders: %v\n", folders)

	// test if a sourceFolder already used in the past and give warning
	datasetIngestor.TestForExistingSourceFolder(folders, client, APIServer, user["accessToken"], allowExistingSourceFolder)

	// TODO ask archive system if sourcefolder is known to them. If yes no copy needed, otherwise
	// a destination location is defined by the archive system
	// for now let the user decide if he needs a copy

	// now everything is prepared, start to loop over all folders
	var skip = ""
	var datasetList []string
	for _, sourceFolder := range folders {
		// ignore empty lines
		if sourceFolder == "" {
			continue
		}
		metaDataMap["sourceFolder"] = sourceFolder

		log.Printf("Scanning files in dataset %s", sourceFolder)

		// check if skip flag is globally defined via flags:
		if linkfiles != nil {
			skip = "dA" // default behaviour = keep internal for all
			if *linkfiles == "delete" {
				skip = "sA"
			} else if *linkfiles == "keep" {
				skip = "kA"
			}
		}

		fullFileArray, startTime, endTime, owner, numFiles, totalSize :=
			datasetIngestor.AssembleFilelisting(sourceFolder, filelistingPath, &skip)
		//log.Printf("full fileListing: %v\n Start and end time: %s %s\n ", fullFileArray, startTime, endTime)
		log.Printf("The dataset contains %v files with a total size of %v bytes.", numFiles, totalSize)

		if totalSize == 0 {
			emptyDatasets++
			color.Set(color.FgRed)
			log.Println("This dataset contains no files and will therefore NOT be stored. ")
			color.Unset()
		} else if numFiles > TOTAL_MAXFILES {
			tooLargeDatasets++
			color.Set(color.FgRed)
			log.Printf("This dataset exceeds the current filecount limit of the archive system of %v files and will therefore NOT be stored.\n", TOTAL_MAXFILES)
			color.Unset()
		} else {
			datasetIngestor.UpdateMetaData(client, APIServer, user, originalMap, metaDataMap, startTime, endTime, owner, tapecopies)
			pretty, _ := json.MarshalIndent(metaDataMap, "", "    ")

			log.Printf("Updated metadata object:\n%s\n", pretty)

			// check if data is accesible at archive server, unless beamline account (assumed to be centrally available always)
			// and unless copy flag defined via command line
			if !*copyFlag && !*nocopyFlag {
				if !beamlineAccount {
					err := datasetIngestor.CheckDataCentrallyAvailable(user["username"], RSYNCServer, sourceFolder)
					if err != nil {
						color.Set(color.FgYellow)
						log.Printf("The source folder %v is not centrally available (decentral use case).\nThe data must first be copied to a rsync cache server.\n ", sourceFolder)
						color.Unset()
						*copyFlag = true
						// check if user account
						if len(accessGroups) == 0 {
							color.Set(color.FgRed)
							log.Println("For the decentral case you must use a personal account. Beamline accounts are not supported.")
							color.Unset()
							os.Exit(1)
						}
						if !*noninteractiveFlag {
							log.Printf("Do you want to continue (Y/n)? ")
							scanner.Scan()
							continueFlag := scanner.Text()
							if continueFlag == "n" {
								log.Fatalln("Further ingests interrupted because decentral case detected, but no copy wanted.")
							}
						}
					}
				} else {
					*copyFlag = false
				}
			} else {
				if !*copyFlag {
					*copyFlag = !*nocopyFlag
				}
			}
			if *ingestFlag {
				// create ingest . For decentral case delay setting status to archivable until data is copied
				archivable := false
				if _, ok := metaDataMap["datasetlifecycle"]; !ok {
					metaDataMap["datasetlifecycle"] = map[string]interface{}{}
				}
				if *copyFlag {
					// do not override existing fields
					metaDataMap["datasetlifecycle"].(map[string]interface{})["isOnCentralDisk"] = false
					metaDataMap["datasetlifecycle"].(map[string]interface{})["archiveStatusMessage"] = "filesNotYetAvailable"
					metaDataMap["datasetlifecycle"].(map[string]interface{})["archivable"] = archivable
				} else {
					archivable = true
					metaDataMap["datasetlifecycle"].(map[string]interface{})["isOnCentralDisk"] = true
					metaDataMap["datasetlifecycle"].(map[string]interface{})["archiveStatusMessage"] = "datasetCreated"
					metaDataMap["datasetlifecycle"].(map[string]interface{})["archivable"] = archivable
				}
				datasetId := datasetIngestor.SendIngestCommand(client, APIServer, metaDataMap, fullFileArray, user)
				// add attachment optionally
				if *addAttachment != "" {
					datasetIngestor.AddAttachment(client, APIServer, datasetId, metaDataMap, user["accessToken"], *addAttachment, *addCaption)
				}
				if *copyFlag {
					err := datasetIngestor.SyncDataToFileserver(datasetId, user, RSYNCServer, sourceFolder, absFileListing)
					if err == nil {
						// delayed enabling
						archivable = true
						datasetIngestor.SendFilesReadyCommand(client, APIServer, datasetId, user)
					} else {
						color.Set(color.FgRed)
						log.Printf("The  command to copy files exited with error %v \n", err)
						log.Printf("The dataset %v is not yet in an archivable state\n", datasetId)
						// TODO let user decide to delete dataset entry
						// datasetIngestor.DeleteDatasetEntry(client, APIServer, datasetId, user["accessToken"])
						color.Unset()
					}
				}

				if archivable {
					datasetList = append(datasetList, datasetId)
				}
			}
			datasetIngestor.ResetUpdatedMetaData(originalMap, metaDataMap)
		}
	}

	if !*ingestFlag {
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
	datasetIngestor.PrintFileInfos()

	// stop here if empty datasets appeared
	if emptyDatasets > 0 || tooLargeDatasets > 0 {
		os.Exit(1)
	}
	// start archive job
	if *autoarchiveFlag && *ingestFlag {
		log.Printf("Submitting Archive Job for the ingested datasets.\n")
		datasetUtils.CreateJob(client, APIServer, user, datasetList, tapecopies)
	}

	// print out results to STDOUT, one line per dataset
	for i := 0; i < len(datasetList); i++ {
		fmt.Println(datasetList[i])
	}
}
