package orchestrator

import (
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/SwissOpenEM/globus"
	"github.com/paulscherrerinstitute/scicat-cli/v3/cmd/cliutils"
	"github.com/paulscherrerinstitute/scicat-cli/v3/datasetIngestor"
	"github.com/paulscherrerinstitute/scicat-cli/v3/datasetUtils"
	"github.com/spf13/cobra"
)

func makeIngestPipelineTestCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "datasetIngestor"}
	cmd.Flags().Bool("ingest", false, "")
	cmd.Flags().Bool("testenv", false, "")
	cmd.Flags().Bool("devenv", false, "")
	cmd.Flags().Bool("localenv", false, "")
	cmd.Flags().Bool("tunnelenv", false, "")
	cmd.Flags().String("scicat-url", "", "")
	cmd.Flags().String("rsync-url", "", "")
	cmd.Flags().Bool("noninteractive", false, "")
	cmd.Flags().String("user", "", "")
	cmd.Flags().String("token", "", "")
	cmd.Flags().Bool("oidc", false, "")
	cmd.Flags().Bool("copy", false, "")
	cmd.Flags().Bool("nocopy", false, "")
	cmd.Flags().String("transfer-type", "ssh", "")
	cmd.Flags().Int("tapecopies", 0, "")
	cmd.Flags().Bool("autoarchive", false, "")
	cmd.Flags().String("linkfiles", "keepInternalOnly", "")
	cmd.Flags().Bool("allowexistingsource", false, "")
	cmd.Flags().String("addattachment", "", "")
	cmd.Flags().String("addcaption", "", "")
	cmd.Flags().Bool("version", false, "")
	cmd.Flags().String("globus-cfg", "", "")
	return cmd
}

func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	oldStdout := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("failed to create stdout pipe: %v", err)
	}
	os.Stdout = w

	fn()

	_ = w.Close()
	os.Stdout = oldStdout

	bytes, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("failed to read captured stdout: %v", err)
	}
	return string(bytes)
}

func TestRunIngestionPipelineInvokesTestFlagsHook(t *testing.T) {
	prevFlagsHook := datasetUtils.TestFlags
	prevArgsHook := datasetUtils.TestArgs
	defer func() {
		datasetUtils.TestFlags = prevFlagsHook
		datasetUtils.TestArgs = prevArgsHook
	}()

	cmd := makeIngestPipelineTestCmd()
	if err := cmd.Flags().Set("ingest", "true"); err != nil {
		t.Fatalf("failed to set ingest flag: %v", err)
	}
	if err := cmd.Flags().Set("copy", "true"); err != nil {
		t.Fatalf("failed to set copy flag: %v", err)
	}
	if err := cmd.Flags().Set("addattachment", "img.png"); err != nil {
		t.Fatalf("failed to set addattachment flag: %v", err)
	}

	called := false
	var got map[string]interface{}
	datasetUtils.TestFlags = func(flags map[string]interface{}) {
		called = true
		got = flags
	}
	datasetUtils.TestArgs = nil

	RunIngestionPipeline(cmd, []string{"meta.json"}, "v1.2.3")

	if !called {
		t.Fatalf("expected TestFlags hook to be called")
	}
	if got["ingest"] != true || got["copy"] != true || got["addattachment"] != "img.png" {
		t.Fatalf("unexpected flags map: %#v", got)
	}
}

func TestRunIngestionPipelineInvokesTestArgsHook(t *testing.T) {
	prevFlagsHook := datasetUtils.TestFlags
	prevArgsHook := datasetUtils.TestArgs
	defer func() {
		datasetUtils.TestFlags = prevFlagsHook
		datasetUtils.TestArgs = prevArgsHook
	}()

	cmd := makeIngestPipelineTestCmd()
	datasetUtils.TestFlags = nil

	called := false
	var got []interface{}
	datasetUtils.TestArgs = func(args []interface{}) {
		called = true
		got = args
	}

	RunIngestionPipeline(cmd, []string{"meta.json", "folderlisting.txt"}, "v1.2.3")

	if !called {
		t.Fatalf("expected TestArgs hook to be called")
	}
	if len(got) != 3 {
		t.Fatalf("expected 3 args in hook, got %d", len(got))
	}
	if got[0] != "meta.json" || got[1] != "" || got[2] != "folderlisting.txt" {
		t.Fatalf("unexpected parsed args payload: %#v", got)
	}
}

func TestRunIngestionPipelinePrintsVersion(t *testing.T) {
	prevFlagsHook := datasetUtils.TestFlags
	prevArgsHook := datasetUtils.TestArgs
	defer func() {
		datasetUtils.TestFlags = prevFlagsHook
		datasetUtils.TestArgs = prevArgsHook
	}()

	datasetUtils.TestFlags = nil
	datasetUtils.TestArgs = nil

	cmd := makeIngestPipelineTestCmd()
	if err := cmd.Flags().Set("version", "true"); err != nil {
		t.Fatalf("failed to set version flag: %v", err)
	}

	out := captureStdout(t, func() {
		RunIngestionPipeline(cmd, []string{"meta.json"}, "9.9.9")
	})

	if !strings.Contains(out, "9.9.9") {
		t.Fatalf("expected version output to contain 9.9.9, got: %q", out)
	}
}

func TestRunIngestionPipelineMultipleArgsCreatesExpectedArchiveJob(t *testing.T) {
	prevFlagsHook := datasetUtils.TestFlags
	prevArgsHook := datasetUtils.TestArgs
	prevCheckVersion := checkForNewVersionFn
	prevCheckService := checkForServiceAvailabilityFn
	prevAuth := authenticateFn
	prevReadMeta := readAndCheckMetadataFn
	prevTestExisting := testForExistingSourceFolderFn
	prevCheckCentral := checkCentralAvailabilityFn
	prevUpdateMeta := updateMetaDataFn
	prevResetMeta := resetUpdatedMetaDataFn
	prevIngest := ingestDatasetFn
	prevAttach := addAttachmentFn
	prevCreateJob := createArchivalJobFn
	defer func() {
		datasetUtils.TestFlags = prevFlagsHook
		datasetUtils.TestArgs = prevArgsHook
		checkForNewVersionFn = prevCheckVersion
		checkForServiceAvailabilityFn = prevCheckService
		authenticateFn = prevAuth
		readAndCheckMetadataFn = prevReadMeta
		testForExistingSourceFolderFn = prevTestExisting
		checkCentralAvailabilityFn = prevCheckCentral
		updateMetaDataFn = prevUpdateMeta
		resetUpdatedMetaDataFn = prevResetMeta
		ingestDatasetFn = prevIngest
		addAttachmentFn = prevAttach
		createArchivalJobFn = prevCreateJob
	}()

	datasetUtils.TestFlags = nil
	datasetUtils.TestArgs = nil
	checkForNewVersionFn = func(client *http.Client, cmd string, version string) {}
	checkForServiceAvailabilityFn = func(client *http.Client, testenv bool, autoarchive bool) {}
	authenticateFn = func(authenticator cliutils.Authenticator, httpClient *http.Client, apiServer string, userpass string, token string, oidc bool, overrideFatalExit ...func(v ...any)) (map[string]string, []string, error) {
		return map[string]string{"accessToken": "token-123", "username": "alice", "mail": "alice@example.org"}, []string{"group-a"}, nil
	}
	testForExistingSourceFolderFn = func(datasetPaths []string, client *http.Client, apiServer, token string) (datasetIngestor.DatasetQuery, error) {
		return datasetIngestor.DatasetQuery{}, nil
	}
	checkCentralAvailabilityFn = func(username, rsyncServer, sourceFolder string, output io.Writer) (error, error) {
		return nil, nil
	}
	updateMetaDataFn = func(client *http.Client, APIServer string, user map[string]string, originalMap map[string]string, metaDataMap map[string]interface{}, startTime time.Time, endTime time.Time, owner string, tapecopies int) {
		// keep this as a no-op; we only need to avoid external policy calls.
	}
	resetUpdatedMetaDataFn = func(originalMap map[string]string, metaDataMap map[string]interface{}) {}
	addAttachmentFn = func(client *http.Client, apiServer, datasetId string, metaDataMap map[string]interface{}, token, filename, caption string) error {
		return nil
	}

	tmp := t.TempDir()
	metaToSource := map[string]string{
		"m1.json": filepath.Join(tmp, "ds1"),
		"m2.json": filepath.Join(tmp, "ds2"),
		"m3.json": filepath.Join(tmp, "ds3"),
	}
	for _, src := range metaToSource {
		if err := os.MkdirAll(src, 0o755); err != nil {
			t.Fatalf("failed creating source folder: %v", err)
		}
		if err := os.WriteFile(filepath.Join(src, "data.txt"), []byte("payload"), 0o600); err != nil {
			t.Fatalf("failed creating sample data file: %v", err)
		}
	}

	readAndCheckMetadataFn = func(client *http.Client, APIServer string, metadatafile string, user map[string]string, accessGroups []string) (map[string]interface{}, string, bool, error) {
		src, ok := metaToSource[metadatafile]
		if !ok {
			return nil, "", false, errors.New("unexpected metadata file")
		}
		return map[string]interface{}{
			"type":       "raw",
			"ownerGroup": "p12345",
		}, src, false, nil
	}

	ingestDatasetFn = func(client *http.Client, APIServer string, metaDataMap map[string]interface{}, fullFileArray []datasetIngestor.Datafile, user map[string]string) (string, error) {
		sourceFolder, _ := metaDataMap["sourceFolder"].(string)
		if sourceFolder == "" {
			return "", errors.New("sourceFolder not set in metadata")
		}
		return "pid-" + filepath.Base(sourceFolder), nil
	}

	var jobCalled bool
	var gotOwnerGroup string
	var gotDatasetList []string
	var gotTapeCopies int
	createArchivalJobFn = func(client *http.Client, APIServer string, user map[string]string, ownerGroup string, datasetList []string, tapecopies *int, executionTime *time.Time) (string, error) {
		jobCalled = true
		gotOwnerGroup = ownerGroup
		gotDatasetList = append([]string{}, datasetList...)
		if tapecopies != nil {
			gotTapeCopies = *tapecopies
		}
		return "job-123", nil
	}

	cmd := makeIngestPipelineTestCmd()
	if err := cmd.Flags().Set("ingest", "true"); err != nil {
		t.Fatalf("failed to set ingest flag: %v", err)
	}
	if err := cmd.Flags().Set("autoarchive", "true"); err != nil {
		t.Fatalf("failed to set autoarchive flag: %v", err)
	}
	if err := cmd.Flags().Set("nocopy", "true"); err != nil {
		t.Fatalf("failed to set nocopy flag: %v", err)
	}
	if err := cmd.Flags().Set("tapecopies", "2"); err != nil {
		t.Fatalf("failed to set tapecopies flag: %v", err)
	}

	RunIngestionPipeline(cmd, []string{"m1.json", "m2.json", "m3.json"}, "v1.2.3")

	if !jobCalled {
		t.Fatalf("expected CreateArchivalJob to be called")
	}
	if gotOwnerGroup != "p12345" {
		t.Fatalf("expected ownerGroup p12345, got %q", gotOwnerGroup)
	}
	if gotTapeCopies != 2 {
		t.Fatalf("expected tapecopies=2, got %d", gotTapeCopies)
	}

	expected := []string{"pid-ds1", "pid-ds2", "pid-ds3"}
	sort.Strings(expected)
	sort.Strings(gotDatasetList)
	if strings.Join(gotDatasetList, ",") != strings.Join(expected, ",") {
		t.Fatalf("unexpected archive dataset list: got=%#v expected=%#v", gotDatasetList, expected)
	}
}

func TestParseAndValidateArgs(t *testing.T) {
	t.Run("single arg", func(t *testing.T) {
		got := ParseAndValidateArgs([]string{"meta.json"})
		if got.MetadataFile != "meta.json" {
			t.Fatalf("unexpected metadata file: %s", got.MetadataFile)
		}
		if got.DatasetFileListTxt != "" || got.FolderListingTxt != "" || got.AbsFileListing != "" {
			t.Fatalf("unexpected optional args for single arg case: %#v", got)
		}
	})

	t.Run("dataset file list", func(t *testing.T) {
		got := ParseAndValidateArgs([]string{"meta.json", "list.txt"})
		if got.DatasetFileListTxt != "list.txt" {
			t.Fatalf("expected dataset file list to be set")
		}
		expectedAbs, _ := filepath.Abs("list.txt")
		if got.AbsFileListing != expectedAbs {
			t.Fatalf("expected abs file listing %s, got %s", expectedAbs, got.AbsFileListing)
		}
	})

	t.Run("folder listing", func(t *testing.T) {
		got := ParseAndValidateArgs([]string{"meta.json", "folderlisting.txt"})
		if got.FolderListingTxt != "folderlisting.txt" {
			t.Fatalf("expected folder listing to be set")
		}
		if got.DatasetFileListTxt != "" {
			t.Fatalf("dataset file list should be empty when folderlisting is provided")
		}
	})
}

func TestParseAndValidateSeparatorArgs(t *testing.T) {
	t.Run("metadata and filelist in one arg", func(t *testing.T) {
		got, err := ParseAndValidateSeparatorArgs([]string{"meta.json:list.txt"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got.MetadataFile != "meta.json" {
			t.Fatalf("unexpected metadata file: %s", got.MetadataFile)
		}
		if got.DatasetFileListTxt != "list.txt" {
			t.Fatalf("unexpected dataset file list: %s", got.DatasetFileListTxt)
		}
		expectedAbs, _ := filepath.Abs("list.txt")
		if got.AbsFileListing != expectedAbs {
			t.Fatalf("expected abs file listing %s, got %s", expectedAbs, got.AbsFileListing)
		}
	})

	t.Run("metadata and folderlisting in one arg", func(t *testing.T) {
		got, err := ParseAndValidateSeparatorArgs([]string{"meta.json:folderlisting.txt"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got.MetadataFile != "meta.json" {
			t.Fatalf("unexpected metadata file: %s", got.MetadataFile)
		}
		if got.FolderListingTxt != "folderlisting.txt" {
			t.Fatalf("unexpected folder listing: %s", got.FolderListingTxt)
		}
		if got.DatasetFileListTxt != "" {
			t.Fatalf("expected dataset file list to be empty, got %s", got.DatasetFileListTxt)
		}
	})

	t.Run("invalid separator args", func(t *testing.T) {
		_, err := ParseAndValidateSeparatorArgs([]string{"meta.json"})
		if err == nil {
			t.Fatalf("expected error for args without separator")
		}
	})
}

func TestParseAndValidateAllArgs(t *testing.T) {
	t.Run("legacy two-argument mode", func(t *testing.T) {
		got, err := ParseAndValidateAllArgs([]string{"meta.json", "list.txt"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(got) != 1 {
			t.Fatalf("expected exactly one parsed target, got %d", len(got))
		}
		if got[0].MetadataFile != "meta.json" || got[0].DatasetFileListTxt != "list.txt" {
			t.Fatalf("unexpected parsed target: %#v", got[0])
		}
	})

	t.Run("multi target separator mode", func(t *testing.T) {
		got, err := ParseAndValidateAllArgs([]string{"m1.json:l1.txt", "m2.json:folderlisting.txt"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(got) != 2 {
			t.Fatalf("expected 2 parsed targets, got %d", len(got))
		}
		if got[0].MetadataFile != "m1.json" || got[0].DatasetFileListTxt != "l1.txt" {
			t.Fatalf("unexpected first target: %#v", got[0])
		}
		if got[1].MetadataFile != "m2.json" || got[1].FolderListingTxt != "folderlisting.txt" {
			t.Fatalf("unexpected second target: %#v", got[1])
		}
	})

	t.Run("two standalone json args use legacy mode", func(t *testing.T) {
		got, err := ParseAndValidateAllArgs([]string{"m1.json", "m2.json"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(got) != 1 {
			t.Fatalf("expected 1 parsed target in legacy mode, got %d", len(got))
		}
		if got[0].MetadataFile != "m1.json" || got[0].DatasetFileListTxt != "m2.json" {
			t.Fatalf("unexpected parsed target in legacy mode: %#v", got[0])
		}
	})

	t.Run("empty args", func(t *testing.T) {
		_, err := ParseAndValidateAllArgs([]string{})
		if err == nil {
			t.Fatalf("expected error for empty args")
		}
	})

	t.Run("invalid arg format", func(t *testing.T) {
		_, err := ParseAndValidateAllArgs([]string{"meta.json:list.txt", "bad-arg"})
		if err == nil {
			t.Fatalf("expected error for invalid argument format")
		}
	})
}

func TestResolveDatasetPathsWithoutListing(t *testing.T) {
	got := ResolveDatasetPaths("/data/source", "")
	if len(got) != 1 || got[0] != "/data/source" {
		t.Fatalf("unexpected dataset paths: %#v", got)
	}
}

func TestResolveDatasetPathsWithListing(t *testing.T) {
	tmp := t.TempDir()
	listPath := filepath.Join(tmp, "folderlisting.txt")
	content := "# c1\n/data/a\n\n/data/b\n"
	if err := os.WriteFile(listPath, []byte(content), 0o600); err != nil {
		t.Fatalf("failed writing listing: %v", err)
	}

	got := ResolveDatasetPaths("/unused", listPath)
	if len(got) != 2 || got[0] != "/data/a" || got[1] != "/data/b" {
		t.Fatalf("unexpected dataset paths: %#v", got)
	}
}

func TestInitializeLifecycleFields(t *testing.T) {
	meta := map[string]interface{}{}
	archivable := InitializeLifecycleFields(meta, false)
	if !archivable {
		t.Fatalf("expected archivable true when copy is not required")
	}
	lc := meta["datasetlifecycle"].(map[string]interface{})
	if lc["isOnCentralDisk"] != true || lc["archiveStatusMessage"] != "datasetCreated" || lc["archivable"] != true {
		t.Fatalf("unexpected lifecycle state: %#v", lc)
	}

	meta2 := map[string]interface{}{}
	archivable = InitializeLifecycleFields(meta2, true)
	if archivable {
		t.Fatalf("expected archivable false when copy is required")
	}
	lc2 := meta2["datasetlifecycle"].(map[string]interface{})
	if lc2["isOnCentralDisk"] != false || lc2["archiveStatusMessage"] != "filesNotYetAvailable" || lc2["archivable"] != false {
		t.Fatalf("unexpected lifecycle state: %#v", lc2)
	}
}

func TestVerifyCentralAvailability(t *testing.T) {
	cfg := IngestConfig{NoninteractiveFlag: true}
	user := map[string]string{"username": "alice"}

	copyRequired := VerifyCentralAvailability(cfg, "rsync.psi.ch", "/data/run1", user, []string{"g1"}, func(username, rsyncServer, sourceFolder string, output io.Writer) (error, error) {
		return nil, nil
	})
	if copyRequired {
		t.Fatalf("expected copyRequired=false when sshErr is nil")
	}

	copyRequired = VerifyCentralAvailability(cfg, "rsync.psi.ch", "/data/run1", user, []string{"g1"}, func(username, rsyncServer, sourceFolder string, output io.Writer) (error, error) {
		return errors.New("not available"), nil
	})
	if !copyRequired {
		t.Fatalf("expected copyRequired=true when sshErr is returned")
	}
}

func TestRegisterDatasetWithCatalog(t *testing.T) {
	calledAttach := false
	datasetID := RegisterDatasetWithCatalog(
		&http.Client{},
		"https://api.example.org",
		map[string]interface{}{"type": "raw"},
		FileContext{FullFileArray: []datasetIngestor.Datafile{{Path: "a"}}},
		map[string]string{"accessToken": "tkn"},
		IngestConfig{AddAttachment: "img.png", AddCaption: "caption"},
		func(client *http.Client, apiServer string, metaDataMap map[string]interface{}, fullFileArray []datasetIngestor.Datafile, user map[string]string) (string, error) {
			if len(fullFileArray) != 1 || fullFileArray[0].Path != "a" {
				t.Fatalf("unexpected file payload: %#v", fullFileArray)
			}
			return "pid-123", nil
		},
		func(client *http.Client, apiServer, datasetId string, metaDataMap map[string]interface{}, token, filename, caption string) error {
			calledAttach = true
			if datasetId != "pid-123" || token != "tkn" || filename != "img.png" || caption != "caption" {
				t.Fatalf("unexpected attachment args")
			}
			return nil
		},
	)

	if datasetID != "pid-123" {
		t.Fatalf("unexpected dataset id: %s", datasetID)
	}
	if !calledAttach {
		t.Fatalf("expected attachment function to be called")
	}
}

func TestExecuteFileTransferBuildsParams(t *testing.T) {
	ctx := FileContext{FullFileArray: []datasetIngestor.Datafile{{Path: "x/file1", IsSymlink: false}, {Path: "x/link", IsSymlink: true}}}
	called := false
	archivable := ExecuteFileTransfer(
		&http.Client{},
		"https://api.example.org",
		"rsync.psi.ch",
		"pid-1",
		"/data/run1",
		"/tmp/list.txt",
		map[string]string{"username": "alice"},
		ctx,
		func(params cliutils.TransferParams) (bool, error) {
			called = true
			if params.DatasetId != "pid-1" || params.DatasetSourceFolder != "/data/run1" {
				t.Fatalf("unexpected dataset params")
			}
			if params.SshParams.RsyncServer != "rsync.psi.ch" || params.SshParams.AbsFilelistPath != "/tmp/list.txt" {
				t.Fatalf("unexpected ssh params")
			}
			if len(params.GlobusParams.Filelist) != 2 || params.GlobusParams.Filelist[1] != "x/link" {
				t.Fatalf("unexpected globus file list: %#v", params.GlobusParams.Filelist)
			}
			if len(params.GlobusParams.IsSymlinkList) != 2 || params.GlobusParams.IsSymlinkList[0] || !params.GlobusParams.IsSymlinkList[1] {
				t.Fatalf("unexpected symlink list: %#v", params.GlobusParams.IsSymlinkList)
			}
			return true, nil
		},
		globus.GlobusClient{},
		cliutils.GlobusConfig{},
		"ssh",
	)

	if !called {
		t.Fatalf("expected transfer function to be called")
	}
	if !archivable {
		t.Fatalf("expected archivable result from transfer function")
	}
}

func TestExecuteFileTransferReturnsFalseOnError(t *testing.T) {
	ctx := FileContext{FullFileArray: []datasetIngestor.Datafile{{Path: "x/file1", IsSymlink: false}}}
	archivable := ExecuteFileTransfer(
		&http.Client{},
		"https://api.example.org",
		"rsync.psi.ch",
		"pid-1",
		"/data/run1",
		"/tmp/list.txt",
		map[string]string{"username": "alice"},
		ctx,
		func(params cliutils.TransferParams) (bool, error) {
			return false, errors.New("transfer failed")
		},
		globus.GlobusClient{},
		cliutils.GlobusConfig{},
		"ssh",
	)

	if archivable {
		t.Fatalf("expected archivable=false when transfer returns error")
	}
}

func TestGatherFilesSuccess(t *testing.T) {
	tmp := t.TempDir()
	if err := os.WriteFile(filepath.Join(tmp, "f1.txt"), []byte("payload"), 0o600); err != nil {
		t.Fatalf("failed to create file: %v", err)
	}

	skip := ""
	var skipped uint
	var illegal uint
	ctx, err := GatherFiles(tmp, "", &skip, &skipped, &illegal)
	if err != nil {
		t.Fatalf("unexpected gather error: %v", err)
	}
	if len(ctx.FullFileArray) == 0 {
		t.Fatalf("expected at least one collected file")
	}
	if ctx.TotalSize == 0 {
		t.Fatalf("expected non-zero total size")
	}
	if ctx.StartTime == (time.Time{}) || ctx.EndTime == (time.Time{}) {
		t.Fatalf("expected non-zero time bounds")
	}
}
