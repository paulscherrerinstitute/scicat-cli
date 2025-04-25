package cmd

import (
	"crypto/tls"
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

var globusCheckTransfer = &cobra.Command{
	Use:   "globusCheckTransfer [options] (transfer_task_id transfer_task_id ...)",
	Short: "Checks whether a list of Globus transfers has finished",
	Long: `Tool for checking whether a list of Globus transfers has finished

You must have a Globus account with access to the desired transfers. Optionally,
you can save 

For further help see "` + MANUAL + `"`,
	Args: minArgsWithVersionException(1),
	Run: func(cmd *cobra.Command, args []string) {
		// consts & vars
		var client = &http.Client{
			Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: false}},
			Timeout:   10 * time.Second}

		var APIServer string = PROD_API_SERVER

		// pass parameters
		testenvFlag, _ := cmd.Flags().GetBool("testenv")
		devenvFlag, _ := cmd.Flags().GetBool("devenv")
		localenvFlag, _ := cmd.Flags().GetBool("localenv")
		tunnelenvFlag, _ := cmd.Flags().GetBool("tunnelenv")
		scicatUrl, _ := cmd.Flags().GetString("scicat-url")
		userpass, _ := cmd.Flags().GetString("user")
		token, _ := cmd.Flags().GetString("token")
		oidc, _ := cmd.Flags().GetBool("oidc")
		showVersion, _ := cmd.Flags().GetBool("version")
		globusCfgFlag, _ := cmd.Flags().GetString("globus-cfg")
		markArchivable, _ := cmd.Flags().GetBool("mark-archivable")
		dryRun, _ := cmd.Flags().GetBool("dry-run")
		autoarchiveFlag, _ := cmd.Flags().GetBool("autoarchive")
		skipDestPathCheck, _ := cmd.Flags().GetBool("skip-dest-path-check")
		tapecopies, _ := cmd.Flags().GetInt("tapecopies")

		if datasetUtils.TestFlags != nil {
			datasetUtils.TestFlags(map[string]interface{}{
				"testenv":              testenvFlag,
				"devenv":               devenvFlag,
				"localenv":             localenvFlag,
				"tunnelenv":            tunnelenvFlag,
				"user":                 userpass,
				"token":                token,
				"version":              showVersion,
				"globus-cfg":           globusCfgFlag,
				"mark-archivable":      globusCfgFlag,
				"dry-run":              dryRun,
				"autoarchive":          autoarchiveFlag,
				"skip-dest-path-check": skipDestPathCheck,
				"tapecopies":           tapecopies,
			})
			return
		}

		// === execute command ===

		// show version
		if showVersion {
			fmt.Printf("%s\n", VERSION)
			return
		}

		datasetUtils.CheckForNewVersion(client, "dummystring", VERSION)

		// since Cobra doesn't support one way dependent flags, we have to do this:
		if !markArchivable && (dryRun || autoarchiveFlag || tapecopies > 0) {
			log.Fatalln("Can't use \"dry-run\", \"autoarchive\" or \"tapecopies\" if \"mark-archivable\" is not set.")
		}
		if !autoarchiveFlag && tapecopies > 0 {
			log.Fatalln("Can't use \"tapecopies\" if \"autoarchive\" is not set.")
		}

		// find globus config
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

		// environment overrides
		if tunnelenvFlag {
			APIServer = TUNNEL_API_SERVER
		}
		if localenvFlag {
			APIServer = LOCAL_API_SERVER
		}
		if devenvFlag {
			APIServer = DEV_API_SERVER
		}
		if testenvFlag {
			APIServer = TEST_API_SERVER
		}
		if scicatUrl != "" {
			APIServer = scicatUrl
		}

		// start message
		startMessage := "Checking transfer complpetion"
		if markArchivable {
			startMessage += ", archivability"
		}
		if autoarchiveFlag {
			startMessage += " and attempting auto-archival"
		}
		startMessage += " of the datasets corresponding to the following transfer tasks:\n"

		color.Set(color.FgGreen)
		log.Println(startMessage)
		for _, task := range args {
			fmt.Printf(" - %s\n", task)
		}
		color.Unset()

		// logging into scicat and globus...
		var user map[string]string
		if markArchivable {
			var err error
			user, _, err = authenticate(RealAuthenticator{}, client, APIServer, userpass, token, oidc)
			if err != nil {
				log.Fatal(err)
			}
		}

		globusClient, gConfig, err := cliutils.GlobusLogin(globusConfigPath)
		if err != nil {
			log.Fatalf("Couldn't create globus client: %v\n", err)
		}

		// go through each transfer task, and execute the requested operations
		archivableDatasetMap := make(map[string][]string)
		for _, taskId := range args {
			groupedDatasets := globusCheckTransferHandleTransferTask(globusClient, taskId, markArchivable, gConfig, skipDestPathCheck, dryRun, client, APIServer, user)
			for group := range groupedDatasets {
				if _, ok := archivableDatasetMap[group]; ok {
					archivableDatasetMap[group] = append(archivableDatasetMap[group], groupedDatasets[group]...)
				} else {
					archivableDatasetMap[group] = groupedDatasets[group]
				}
			}
		}

		// === create archive job ===
		if autoarchiveFlag {
			globusCheckTransferCreateArchiveJobs(client, APIServer, user, archivableDatasetMap, tapecopies)
		}
	},
}

func init() {
	rootCmd.AddCommand(globusCheckTransfer)

	globusCheckTransfer.Flags().Bool("testenv", false, "Use test environment (qa) instead of production environment")
	globusCheckTransfer.Flags().Bool("devenv", false, "Use development environment instead of production environment (developers only)")
	globusCheckTransfer.Flags().Bool("localenv", false, "Use local environment instead of production environment (developers only)")
	globusCheckTransfer.Flags().Bool("tunnelenv", false, "Use tunneled API server at port 5443 to access development instance (developers only)")
	globusCheckTransfer.Flags().String("globus-cfg", "", "Override globus transfer config file location [default: globus.yaml next to executable]")
	globusCheckTransfer.Flags().Bool("mark-archivable", false, "")
	globusCheckTransfer.Flags().Bool("dry-run", false, "")
	globusCheckTransfer.Flags().Bool("autoarchive", false, "")
	globusCheckTransfer.Flags().Bool("skip-dest-path-check", false, "")
	globusCheckTransfer.Flags().Int("tapecopies", 0, "Number of tapecopies to be used for archiving")

	globusCheckTransfer.MarkFlagsMutuallyExclusive("testenv", "devenv", "localenv", "tunnelenv", "scicat-url")
	globusCheckTransfer.MarkFlagsMutuallyExclusive("dry-run", "autoarchive")
	globusCheckTransfer.MarkFlagsMutuallyExclusive("dry-run", "tapecopies")
}

func globusCheckTransferCreateArchiveJobs(client *http.Client, APIServer string, user map[string]string, archivableDatasetMap map[string][]string, tapecopies int) {
	log.Printf("Submitting Archive Job for archivable datasets.\n")
	// TODO: change param type from pointer to regular as it is unnecessary
	//   for it to be passed as pointer
	jobIds, errs := datasetUtils.CreateArchivalJobs(client, APIServer, user, archivableDatasetMap, &tapecopies)

	color.Set(color.FgRed)
	for _, err := range errs {
		if err != nil {
			// normally this only happens if either not the same user checks the completion, the tool somehow adds others' datasets into the list or
			// the user lost access groups in the mean time
			log.Printf("Could not create the archival job for a set of the ingested datasets: %s\n", err.Error())
		}
	}
	color.Unset()

	log.Println("Submitted jobs:", jobIds)
}

func globusCheckTransferHandleTransferTask(
	globusClient globus.GlobusClient,
	taskId string, markArchivable bool,
	gConfig cliutils.GlobusConfig,
	skipDestPathCheck bool,
	dryRun bool,
	client *http.Client,
	APIServer string,
	user map[string]string,
) (archivableDatasetMap map[string][]string) {
	archivableDatasetMap = make(map[string][]string)

	task, err := globusClient.TransferGetTaskByID(taskId)
	if err != nil {
		log.Printf("Transfer task with ID \"%s\" returned error: %v\n", taskId, err)
		return nil
	}
	fmt.Printf("Task status: \n=====\n%v\n=====\n", task)

	// if marking as archivable is requested and the transfer has succeeded
	if markArchivable && task.Status == "SUCCEEDED" {
		if task.SourceBasePath == nil {
			log.Printf("Can't get source base path for task \"%s\". It will not be marked as archivable, but can probably be archived.\n", taskId)
			return nil
		}

		// get source and dest folders
		sourceFolder := *task.SourceBasePath
		sourceFolder = strings.TrimPrefix(sourceFolder, gConfig.SourcePrefixPath)
		sourceFolder = strings.TrimSuffix(sourceFolder, "/")
		var destFolder string
		if !skipDestPathCheck {
			if task.DestinationBasePath == nil {
				log.Printf("Can't get destination base path for task \"%s\". It will not be marked as archivable, but can probably be archived.\n", taskId)
				return nil
			}
			destFolder = *task.DestinationBasePath
		}

		list, err := datasetIngestor.TestForExistingSourceFolder([]string{sourceFolder}, client, APIServer, user["accessToken"])

		// error handling and exceptions
		if err != nil {
			log.Printf("WARNING - an error has occurred when querying the sourcefolder \"%s\" of task id \"%s\": %v\n", sourceFolder, taskId, err)
			log.Printf("Can't set %s task's dataset to archivable.\n", taskId)
			return nil
		}
		if len(list) <= 0 {
			log.Printf("WARNING - empty dataset list returned for the sourcefolder \"%s\" of task id \"%s\": %v\n", sourceFolder, taskId, err)
			log.Printf("Can't set %s task's dataset to archivable.\n", taskId)
			return nil
		}
		if dryRun {
			log.Println("list of found datasets:")
			for _, result := range list {
				fmt.Printf(" - %s\n", result.Pid)
			}
			log.Println("since dry-run is set, the command will not attempt to mark the above datasets as archivable, or try to archive them")
			return nil
		}

		for _, result := range list {
			if !skipDestPathCheck {
				separatedPid := strings.Split(result.Pid, "/")
				if len(separatedPid) != 2 {
					log.Printf("\"%s\" dataset has irregular PID. Cannot check destFolder with it. Skipping...\n", result.Pid)
					continue
				}
				shortPid := separatedPid[1]
				if !strings.Contains(destFolder, shortPid) {
					log.Printf("\"%s\" dataset's PID does not appear in the destination folder (\"%s\"). Cannot mark it as archivable. Skipping...\n", result.Pid, destFolder)
					continue
				}
			}
			log.Printf("%s dataset is being marked as archivable...\n", result.Pid)
			err := datasetIngestor.MarkFilesReady(client, APIServer, result.Pid, user)
			if err != nil {
				log.Printf("WARNING - error occurred while trying to mark files ready for dataset with PID \"%s\": %v\n", result.Pid, err)
				log.Printf("%s dataset was (likely) not marked archivable.\n", result.Pid)
				continue
			}
			log.Printf("%s dataset was successfully marked as archivable.\n", result.Pid)
			if _, ok := archivableDatasetMap[result.OwnerGroup]; ok {
				archivableDatasetMap[result.OwnerGroup] = append(archivableDatasetMap[result.OwnerGroup], result.Pid)
			} else {
				archivableDatasetMap[result.OwnerGroup] = []string{result.Pid}
			}
		}
	}

	// if marking as archivable is requested but the transfer has *not* succeeded
	if markArchivable && task.Status != "SUCCEEDED" {
		log.Printf("%s task's status is %s, the corresponding dataset can't be marked as archivable.\n", taskId, task.Status)
	}

	return archivableDatasetMap
}
