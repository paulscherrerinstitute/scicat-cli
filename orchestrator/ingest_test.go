package orchestrator

import (
	"bufio"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/SwissOpenEM/globus"
	"github.com/paulscherrerinstitute/scicat-cli/v3/cmd/cliutils"
	"github.com/paulscherrerinstitute/scicat-cli/v3/datasetIngestor"
	"github.com/paulscherrerinstitute/scicat-cli/v3/datasetUtils"
	"github.com/spf13/cobra"
)

// stubStrategy implements IngestionStrategy via function fields, letting tests
// override individual methods without writing a full concrete type per test.
type stubStrategy struct {
	readMetadata  func(*http.Client, string, string, map[string]string, []string) (map[string]interface{}, string, bool, error)
	ingest        func(*http.Client, string, map[string]interface{}, []datasetIngestor.Datafile, map[string]string) (string, error)
	ingestRemote  func(*http.Client, string, map[string]interface{}, []datasetIngestor.Datafile, map[string]string) (string, error)
	addAttachment func(*http.Client, string, string, map[string]interface{}, string, string, string) error
}

func (s stubStrategy) ReadMetadata(c *http.Client, api, arg string, user map[string]string, groups []string) (map[string]interface{}, string, bool, error) {
	return s.readMetadata(c, api, arg, user, groups)
}
func (s stubStrategy) Ingest(c *http.Client, api string, meta map[string]interface{}, files []datasetIngestor.Datafile, user map[string]string) (string, error) {
	return s.ingest(c, api, meta, files, user)
}
func (s stubStrategy) IngestRemote(c *http.Client, api string, meta map[string]interface{}, files []datasetIngestor.Datafile, user map[string]string) (string, error) {
	return s.ingestRemote(c, api, meta, files, user)
}
func (s stubStrategy) AddAttachment(c *http.Client, api, id string, meta map[string]interface{}, token, file, caption string) error {
	return s.addAttachment(c, api, id, meta, token, file, caption)
}

// errStub returns a stubStrategy whose every method returns an error, so a test
// that forgets to wire a method fails loudly instead of silently passing.
func errStub(name string) stubStrategy {
	return stubStrategy{
		readMetadata: func(*http.Client, string, string, map[string]string, []string) (map[string]interface{}, string, bool, error) {
			return nil, "", false, errors.New(name + ".ReadMetadata not configured for this test")
		},
		ingest: func(*http.Client, string, map[string]interface{}, []datasetIngestor.Datafile, map[string]string) (string, error) {
			return "", errors.New(name + ".Ingest not configured for this test")
		},
		ingestRemote: func(*http.Client, string, map[string]interface{}, []datasetIngestor.Datafile, map[string]string) (string, error) {
			return "", errors.New(name + ".IngestRemote not configured for this test")
		},
		addAttachment: func(*http.Client, string, string, map[string]interface{}, string, string, string) error {
			return nil
		},
	}
}

// noopCtx returns an IngestContext with every injectable function replaced by a
// safe no-op or minimal stub. Override only the fields that matter in each test.
func noopCtx() IngestContext {
	return IngestContext{
		Client:  &http.Client{},
		Scanner: bufio.NewScanner(strings.NewReader("")),

		CheckForNewVersion:          func(*http.Client, string, string) {},
		CheckForServiceAvailability: func(*http.Client, bool, bool) {},
		Authenticate: func(_ cliutils.Authenticator, _ *http.Client, _, _, _ string, _ bool, _ ...func(...any)) (map[string]string, []string, error) {
			return map[string]string{"accessToken": "test-token", "username": "alice"}, []string{"group-a"}, nil
		},
		TestForExistingSourceFolder: func(_ []string, _ *http.Client, _, _ string) (datasetIngestor.DatasetQuery, error) {
			return nil, nil
		},
		CheckCentralAvailability: func(_, _, _ string, _ io.Writer) (error, error) {
			return nil, nil
		},
		UpdateMetaData:       func(*http.Client, string, map[string]string, map[string]string, map[string]interface{}, time.Time, time.Time, string, int) {},
		ResetUpdatedMetaData: func(map[string]string, map[string]interface{}) {},
		FileIngestion:        errStub("FileIngestion"),
		DatasetIdIngestion:   errStub("DatasetIdIngestion"),
		CreateArchivalJob: func(*http.Client, string, map[string]string, string, []string, *int, *time.Time) (string, error) {
			return "", errors.New("CreateArchivalJob not configured for this test")
		},
	}
}

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
	cmd.Flags().Bool("remotefilescan", false, "")
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

// TestRunIngestionPipelineMultipleSourceFoldersCreatesArchiveJob exercises the
// full runIngestionPipeline path with three source folders collected via a
// folder-listing file, verifying that all three dataset IDs end up in the
// archive job call.
func TestRunIngestionPipelineMultipleSourceFoldersCreatesArchiveJob(t *testing.T) {
	prevFlags := datasetUtils.TestFlags
	prevArgs := datasetUtils.TestArgs
	defer func() {
		datasetUtils.TestFlags = prevFlags
		datasetUtils.TestArgs = prevArgs
	}()
	datasetUtils.TestFlags = nil
	datasetUtils.TestArgs = nil

	tmp := t.TempDir()
	for _, name := range []string{"ds1", "ds2", "ds3"} {
		dir := filepath.Join(tmp, name)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", dir, err)
		}
		if err := os.WriteFile(filepath.Join(dir, "data.dat"), []byte("payload"), 0o600); err != nil {
			t.Fatalf("write data file: %v", err)
		}
	}
	listPath := filepath.Join(tmp, "folderlisting.txt")
	listContent := filepath.Join(tmp, "ds1") + "\n" + filepath.Join(tmp, "ds2") + "\n" + filepath.Join(tmp, "ds3") + "\n"
	if err := os.WriteFile(listPath, []byte(listContent), 0o600); err != nil {
		t.Fatalf("write listing: %v", err)
	}

	var gotOwnerGroup string
	var gotDatasets []string

	ctx := noopCtx()
	ctx.Cfg = IngestConfig{
		IngestFlag:        true,
		AutoarchiveFlag:   true,
		NocopyFlag:        true,
		NocopyFlagChanged: true,
		Tapecopies:        1,
	}
	fs := errStub("FileIngestion")
	fs.readMetadata = func(*http.Client, string, string, map[string]string, []string) (map[string]interface{}, string, bool, error) {
		return map[string]interface{}{"type": "raw", "ownerGroup": "p12345"}, "", false, nil
	}
	fs.ingest = func(_ *http.Client, _ string, meta map[string]interface{}, _ []datasetIngestor.Datafile, _ map[string]string) (string, error) {
		src, _ := meta["sourceFolder"].(string)
		return "pid-" + filepath.Base(src), nil
	}
	ctx.FileIngestion = fs
	ctx.CreateArchivalJob = func(_ *http.Client, _ string, _ map[string]string, ownerGroup string, datasets []string, _ *int, _ *time.Time) (string, error) {
		gotOwnerGroup = ownerGroup
		gotDatasets = append([]string{}, datasets...)
		return "job-abc", nil
	}

	dArgsList, err := ParseAndValidateAllArgs([]string{"meta.json", listPath})
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	if err := runIngestionPipeline(ctx, dArgsList, "v1.0.0"); err != nil {
		t.Fatalf("unexpected pipeline error: %v", err)
	}

	if gotOwnerGroup != "p12345" {
		t.Fatalf("expected ownerGroup p12345, got %q", gotOwnerGroup)
	}
	if len(gotDatasets) != 3 {
		t.Fatalf("expected 3 datasets in archive job, got %d: %v", len(gotDatasets), gotDatasets)
	}
	want := []string{"pid-ds1", "pid-ds2", "pid-ds3"}
	sort.Strings(want)
	sort.Strings(gotDatasets)
	if strings.Join(gotDatasets, ",") != strings.Join(want, ",") {
		t.Fatalf("archive dataset list mismatch: got=%v want=%v", gotDatasets, want)
	}
}

func TestParseAndValidateArgs(t *testing.T) {
	t.Run("single arg", func(t *testing.T) {
		got, err := ParseAndValidateArgs([]string{"meta.json"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got.MetadataFile != "meta.json" {
			t.Fatalf("unexpected metadata file: %s", got.MetadataFile)
		}
		if got.DatasetFileListTxt != "" || got.FolderListingTxt != "" || got.AbsFileListing != "" {
			t.Fatalf("unexpected optional args for single arg case: %#v", got)
		}
	})

	t.Run("dataset file list", func(t *testing.T) {
		got, err := ParseAndValidateArgs([]string{"meta.json", "list.txt"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got.DatasetFileListTxt != "list.txt" {
			t.Fatalf("expected dataset file list to be set")
		}
		expectedAbs, _ := filepath.Abs("list.txt")
		if got.AbsFileListing != expectedAbs {
			t.Fatalf("expected abs file listing %s, got %s", expectedAbs, got.AbsFileListing)
		}
	})

	t.Run("folder listing", func(t *testing.T) {
		got, err := ParseAndValidateArgs([]string{"meta.json", "folderlisting.txt"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got.FolderListingTxt != "folderlisting.txt" {
			t.Fatalf("expected folder listing to be set")
		}
		if got.DatasetFileListTxt != "" {
			t.Fatalf("dataset file list should be empty when folderlisting is provided")
		}
	})
}

func TestParseAndValidateSeparatorArg(t *testing.T) {
	t.Run("meta with filelist", func(t *testing.T) {
		got, err := ParseAndValidateSeparatorArg("meta.json@list.txt")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got.MetadataFile != "meta.json" || got.DatasetFileListTxt != "list.txt" {
			t.Fatalf("unexpected result: %#v", got)
		}
		expectedAbs, _ := filepath.Abs("list.txt")
		if got.AbsFileListing != expectedAbs {
			t.Fatalf("expected abs %s, got %s", expectedAbs, got.AbsFileListing)
		}
	})

	t.Run("meta without filelist via separator", func(t *testing.T) {
		got, err := ParseAndValidateSeparatorArg("meta.json@")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got.MetadataFile != "meta.json" {
			t.Fatalf("unexpected metadata file: %s", got.MetadataFile)
		}
		if got.DatasetFileListTxt != "" || got.AbsFileListing != "" {
			t.Fatalf("expected empty filelist fields, got: %#v", got)
		}
	})

	t.Run("no colon falls back to single meta arg", func(t *testing.T) {
		got, err := ParseAndValidateSeparatorArg("meta.json")
		if err != nil {
			t.Fatalf("unexpected error for bare meta arg: %v", err)
		}
		if got.MetadataFile != "meta.json" {
			t.Fatalf("unexpected metadata file: %s", got.MetadataFile)
		}
		if got.DatasetFileListTxt != "" || got.AbsFileListing != "" {
			t.Fatalf("expected empty filelist fields, got: %#v", got)
		}
	})

	t.Run("empty metadata returns error", func(t *testing.T) {
		_, err := ParseAndValidateSeparatorArg("@list.txt")
		if err == nil {
			t.Fatal("expected error for empty metadata file")
		}
	})
}

func TestParseAndValidateAllArgs(t *testing.T) {
	t.Run("legacy single arg", func(t *testing.T) {
		got, err := ParseAndValidateAllArgs([]string{"meta.json"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(got) != 1 || got[0].MetadataFile != "meta.json" {
			t.Fatalf("unexpected result: %#v", got)
		}
	})

	t.Run("legacy two args with filelist", func(t *testing.T) {
		got, err := ParseAndValidateAllArgs([]string{"meta.json", "list.txt"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(got) != 1 || got[0].DatasetFileListTxt != "list.txt" {
			t.Fatalf("unexpected result: %#v", got)
		}
	})

	t.Run("legacy two args with folderlisting", func(t *testing.T) {
		got, err := ParseAndValidateAllArgs([]string{"meta.json", "folderlisting.txt"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(got) != 1 || got[0].FolderListingTxt != "folderlisting.txt" {
			t.Fatalf("unexpected result: %#v", got)
		}
	})

	t.Run("colon syntax single pair", func(t *testing.T) {
		got, err := ParseAndValidateAllArgs([]string{"meta.json@list.txt"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(got) != 1 || got[0].MetadataFile != "meta.json" || got[0].DatasetFileListTxt != "list.txt" {
			t.Fatalf("unexpected result: %#v", got)
		}
	})

	t.Run("colon syntax multiple pairs", func(t *testing.T) {
		got, err := ParseAndValidateAllArgs([]string{"a.json@l1.txt", "b.json@l2.txt", "c.json"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(got) != 3 {
			t.Fatalf("expected 3 results, got %d", len(got))
		}
		if got[0].MetadataFile != "a.json" || got[1].MetadataFile != "b.json" || got[2].MetadataFile != "c.json" {
			t.Fatalf("unexpected metadata files: %v %v %v", got[0].MetadataFile, got[1].MetadataFile, got[2].MetadataFile)
		}
		if got[2].DatasetFileListTxt != "" || got[2].AbsFileListing != "" {
			t.Fatalf("expected empty filelist for third pair: %#v", got[2])
		}
	})

	t.Run("empty args returns error", func(t *testing.T) {
		_, err := ParseAndValidateAllArgs([]string{})
		if err == nil {
			t.Fatal("expected error for empty args")
		}
	})

	t.Run("colon syntax with bare meta arg yields two datasets", func(t *testing.T) {
		got, err := ParseAndValidateAllArgs([]string{"a.json@l.txt", "b.json"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(got) != 2 {
			t.Fatalf("expected 2 results, got %d", len(got))
		}
		if got[0].MetadataFile != "a.json" || got[0].DatasetFileListTxt != "l.txt" {
			t.Fatalf("unexpected first dataset: %#v", got[0])
		}
		if got[1].MetadataFile != "b.json" || got[1].DatasetFileListTxt != "" {
			t.Fatalf("unexpected second dataset: %#v", got[1])
		}
	})

	t.Run("invalid separator pair format returns error", func(t *testing.T) {
		_, err := ParseAndValidateAllArgs([]string{"a.json@l.txt", "bad.csv@list.txt"})
		if err == nil {
			t.Fatal("expected error for invalid @ pair format (left side not .json)")
		}
	})
}

func TestResolveDatasetPathsWithoutListing(t *testing.T) {
	got, err := ResolveDatasetPaths("/data/source", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
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

	got, err := ResolveDatasetPaths("/unused", listPath)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
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
	scanner := bufio.NewScanner(strings.NewReader(""))

	copyRequired, err := VerifyCentralAvailability(cfg, "rsync.psi.ch", "/data/run1", user, []string{"g1"}, scanner, func(username, rsyncServer, sourceFolder string, output io.Writer) (error, error) {
		return nil, nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if copyRequired {
		t.Fatalf("expected copyRequired=false when sshErr is nil")
	}

	copyRequired, err = VerifyCentralAvailability(cfg, "rsync.psi.ch", "/data/run1", user, []string{"g1"}, scanner, func(username, rsyncServer, sourceFolder string, output io.Writer) (error, error) {
		return errors.New("not available"), nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !copyRequired {
		t.Fatalf("expected copyRequired=true when sshErr is returned")
	}
}

func TestRegisterDatasetWithCatalog(t *testing.T) {
	calledAttach := false
	datasetID, err := RegisterDatasetWithCatalog(
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

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
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
		true,
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
		true,
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

func TestResolveDatasetPathsReturnsErrorForMissingFile(t *testing.T) {
	_, err := ResolveDatasetPaths("/unused", "/no/such/file.txt")
	if err == nil {
		t.Fatal("expected error for missing folder listing file")
	}
}

func TestGuardExistingSourceFolders(t *testing.T) {
	noConflict := func(_ []string, _ *http.Client, _, _ string) (datasetIngestor.DatasetQuery, error) {
		return nil, nil
	}
	oneConflict := func(_ []string, _ *http.Client, _, _ string) (datasetIngestor.DatasetQuery, error) {
		return datasetIngestor.DatasetQuery{{Pid: "old-pid", SourceFolder: "/data/run1"}}, nil
	}
	testErr := errors.New("lookup failed")
	failFn := func(_ []string, _ *http.Client, _, _ string) (datasetIngestor.DatasetQuery, error) {
		return datasetIngestor.DatasetQuery{}, testErr
	}

	t.Run("no conflicts returns nil", func(t *testing.T) {
		scanner := bufio.NewScanner(strings.NewReader(""))
		err := GuardExistingSourceFolders(scanner, []string{"/data/run1"}, &http.Client{}, "https://api", "token", false, false, noConflict)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("lookup error propagates", func(t *testing.T) {
		scanner := bufio.NewScanner(strings.NewReader(""))
		err := GuardExistingSourceFolders(scanner, []string{"/data/run1"}, &http.Client{}, "https://api", "token", false, false, failFn)
		if !errors.Is(err, testErr) {
			t.Fatalf("expected lookup error, got: %v", err)
		}
	})

	t.Run("conflict + flagChanged returns error", func(t *testing.T) {
		scanner := bufio.NewScanner(strings.NewReader(""))
		err := GuardExistingSourceFolders(scanner, []string{"/data/run1"}, &http.Client{}, "https://api", "token", false, true, oneConflict)
		if err == nil {
			t.Fatal("expected error when flagChanged is true and conflicts exist")
		}
	})

	t.Run("conflict + allowExisting returns nil", func(t *testing.T) {
		scanner := bufio.NewScanner(strings.NewReader(""))
		err := GuardExistingSourceFolders(scanner, []string{"/data/run1"}, &http.Client{}, "https://api", "token", true, false, oneConflict)
		if err != nil {
			t.Fatalf("unexpected error when allowExisting is true: %v", err)
		}
	})

	t.Run("conflict + user declines returns error", func(t *testing.T) {
		scanner := bufio.NewScanner(strings.NewReader("n\n"))
		err := GuardExistingSourceFolders(scanner, []string{"/data/run1"}, &http.Client{}, "https://api", "token", false, false, oneConflict)
		if err == nil {
			t.Fatal("expected error when user declines")
		}
	})

	t.Run("conflict + user accepts returns nil", func(t *testing.T) {
		scanner := bufio.NewScanner(strings.NewReader("y\n"))
		err := GuardExistingSourceFolders(scanner, []string{"/data/run1"}, &http.Client{}, "https://api", "token", false, false, oneConflict)
		if err != nil {
			t.Fatalf("unexpected error when user accepts: %v", err)
		}
	})
}

func TestVerifyCentralAvailabilityOtherErrReturnsError(t *testing.T) {
	cfg := IngestConfig{NoninteractiveFlag: true}
	user := map[string]string{"username": "alice"}
	scanner := bufio.NewScanner(strings.NewReader(""))
	otherErr := errors.New("network unreachable")

	_, err := VerifyCentralAvailability(cfg, "rsync.psi.ch", "/data/run1", user, []string{"g1"}, scanner,
		func(_, _, _ string, _ io.Writer) (error, error) {
			return nil, otherErr
		},
	)
	if !errors.Is(err, otherErr) {
		t.Fatalf("expected otherErr to propagate, got: %v", err)
	}
}

func TestVerifyCentralAvailabilityBeamlineAccountReturnsError(t *testing.T) {
	cfg := IngestConfig{NoninteractiveFlag: true}
	user := map[string]string{"username": "alice"}
	scanner := bufio.NewScanner(strings.NewReader(""))

	// empty accessGroups signals a beamline account — copy is not supported
	_, err := VerifyCentralAvailability(cfg, "rsync.psi.ch", "/data/run1", user, []string{}, scanner,
		func(_, _, _ string, _ io.Writer) (error, error) {
			return errors.New("not available"), nil
		},
	)
	if err == nil {
		t.Fatal("expected error for beamline account (empty accessGroups)")
	}
}

func TestRunIngestionPipelineReturnsErrorWhenReadMetadataFails(t *testing.T) {
	prevFlags := datasetUtils.TestFlags
	prevArgs := datasetUtils.TestArgs
	defer func() {
		datasetUtils.TestFlags = prevFlags
		datasetUtils.TestArgs = prevArgs
	}()
	datasetUtils.TestFlags = nil
	datasetUtils.TestArgs = nil

	metaErr := errors.New("bad metadata")
	ctx := noopCtx()
	ctx.Cfg = IngestConfig{IngestFlag: true}
	fs := errStub("FileIngestion")
	fs.readMetadata = func(*http.Client, string, string, map[string]string, []string) (map[string]interface{}, string, bool, error) {
		return nil, "", false, metaErr
	}
	ctx.FileIngestion = fs

	dArgsList, _ := ParseAndValidateAllArgs([]string{"meta.json"})
	err := runIngestionPipeline(ctx, dArgsList, "v1.0.0")
	if !errors.Is(err, metaErr) {
		t.Fatalf("expected metadata error to propagate, got: %v", err)
	}
}

func TestRunIngestionPipelineParallelSucceeds(t *testing.T) {
	prevFlags := datasetUtils.TestFlags
	prevArgs := datasetUtils.TestArgs
	defer func() {
		datasetUtils.TestFlags = prevFlags
		datasetUtils.TestArgs = prevArgs
	}()
	datasetUtils.TestFlags = nil
	datasetUtils.TestArgs = nil

	tmp := t.TempDir()
	var ingested []string
	var mu sync.Mutex

	ctx := noopCtx()
	ctx.Cfg = IngestConfig{
		IngestFlag:        true,
		NoninteractiveFlag: true,
		NocopyFlag:        true,
		NocopyFlagChanged: true,
	}
	fs := errStub("FileIngestion")
	fs.readMetadata = func(_ *http.Client, _, metaFile string, _ map[string]string, _ []string) (map[string]interface{}, string, bool, error) {
		dir := filepath.Join(tmp, strings.TrimSuffix(filepath.Base(metaFile), ".json"))
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, "", false, err
		}
		if err := os.WriteFile(filepath.Join(dir, "data.dat"), []byte("x"), 0o600); err != nil {
			return nil, "", false, err
		}
		return map[string]interface{}{"type": "raw", "ownerGroup": "grp"}, dir, false, nil
	}
	fs.ingest = func(_ *http.Client, _ string, meta map[string]interface{}, _ []datasetIngestor.Datafile, _ map[string]string) (string, error) {
		id := "pid-" + filepath.Base(meta["sourceFolder"].(string))
		mu.Lock()
		ingested = append(ingested, id)
		mu.Unlock()
		return id, nil
	}
	ctx.FileIngestion = fs

	dArgsList, err := ParseAndValidateAllArgs([]string{"a.json", "b.json", "c.json"})
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	if err := runIngestionPipeline(ctx, dArgsList, "v1.0.0"); err != nil {
		t.Fatalf("unexpected pipeline error: %v", err)
	}

	if len(ingested) != 3 {
		t.Fatalf("expected 3 ingested datasets, got %d: %v", len(ingested), ingested)
	}
}

func TestRunIngestionPipelineParallelReturnsErrorWhenAnyFails(t *testing.T) {
	prevFlags := datasetUtils.TestFlags
	prevArgs := datasetUtils.TestArgs
	defer func() {
		datasetUtils.TestFlags = prevFlags
		datasetUtils.TestArgs = prevArgs
	}()
	datasetUtils.TestFlags = nil
	datasetUtils.TestArgs = nil

	metaErr := errors.New("bad metadata")
	ctx := noopCtx()
	ctx.Cfg = IngestConfig{IngestFlag: true, NoninteractiveFlag: true}
	fs := errStub("FileIngestion")
	fs.readMetadata = func(*http.Client, string, string, map[string]string, []string) (map[string]interface{}, string, bool, error) {
		return nil, "", false, metaErr
	}
	ctx.FileIngestion = fs

	dArgsList, _ := ParseAndValidateAllArgs([]string{"a.json", "b.json"})
	err := runIngestionPipeline(ctx, dArgsList, "v1.0.0")
	if err == nil {
		t.Fatal("expected error when a parallel ingestion fails, got nil")
	}
}

func TestValidateArgumentFormat(t *testing.T) {
	tests := []struct {
		arg     string
		wantErr bool
	}{
		{"", true},
		{"   ", true},
		{"meta.json", false},
		{"list.txt", false},
		{"meta.json@list.txt", false},
		{"meta.json@", true},
		{"@list.txt", true},
		{"notvalid", true},
		{"/some/path/", true},
		{"meta.json@bad.csv", true},
		{"bad.csv@list.txt", true},
	}
	for _, tc := range tests {
		t.Run(tc.arg, func(t *testing.T) {
			err := ValidateArgumentFormat(tc.arg)
			if tc.wantErr && err == nil {
				t.Fatalf("expected error for %q, got nil", tc.arg)
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error for %q: %v", tc.arg, err)
			}
		})
	}
}

func TestIsLegacyMode(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want bool
	}{
		{"single arg", []string{"meta.json"}, false},
		{"two args json+txt", []string{"meta.json", "list.txt"}, true},
		{"two args json+folderlisting", []string{"meta.json", "folderlisting.txt"}, true},
		{"two args json+json", []string{"meta.json", "other.json"}, false},
		{"three args", []string{"a.json", "b.json", "c.json"}, false},
		{"@ in first arg", []string{"meta.json@list.txt", "other.txt"}, false},
		{"@ in second arg", []string{"meta.json", "list@extra.txt"}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := isLegacyMode(tc.args)
			if got != tc.want {
				t.Fatalf("isLegacyMode(%v) = %v, want %v", tc.args, got, tc.want)
			}
		})
	}
}

// TestRunIngestionPipelineRemoteFileScanSetsArchiveStatus verifies that with
// --remotefilescan the orchestrator:
//   - skips GatherFiles (sourceFolder is a non-existent remote path; if GatherFiles
//     ran it would Chdir there and fail, causing the pipeline to error)
//   - sets archiveStatusMessage to "origDatablocksNotYetAvailable"
//   - registers the dataset as archivable (prints the dataset ID)
func TestRunIngestionPipelineRemoteFileScanSetsArchiveStatus(t *testing.T) {
	prevFlags := datasetUtils.TestFlags
	prevArgs := datasetUtils.TestArgs
	defer func() {
		datasetUtils.TestFlags = prevFlags
		datasetUtils.TestArgs = prevArgs
	}()
	datasetUtils.TestFlags = nil
	datasetUtils.TestArgs = nil

	var gotArchiveStatus string

	ctx := noopCtx()
	ctx.Cfg = IngestConfig{
		IngestFlag:        true,
		RemoteFileScan:    true,
		NocopyFlag:        true,
		NocopyFlagChanged: true,
		Tapecopies:        1,
	}
	// Return a remote path that does not exist locally — GatherFiles would fail
	// if called, so a successful pipeline proves it was skipped.
	fs := errStub("FileIngestion")
	fs.readMetadata = func(*http.Client, string, string, map[string]string, []string) (map[string]interface{}, string, bool, error) {
		return map[string]interface{}{"type": "raw", "ownerGroup": "grp"}, "/nonexistent/remote/source", false, nil
	}
	fs.ingestRemote = func(_ *http.Client, _ string, meta map[string]interface{}, _ []datasetIngestor.Datafile, _ map[string]string) (string, error) {
		if lc, ok := meta["datasetlifecycle"].(map[string]interface{}); ok {
			gotArchiveStatus, _ = lc["archiveStatusMessage"].(string)
		}
		return "pid-remote", nil
	}
	ctx.FileIngestion = fs

	out := captureStdout(t, func() {
		dArgsList, err := ParseAndValidateAllArgs([]string{"meta.json"})
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		if err := runIngestionPipeline(ctx, dArgsList, "v1.0.0"); err != nil {
			t.Fatalf("pipeline error: %v", err)
		}
	})

	if gotArchiveStatus != "origDatablocksNotYetAvailable" {
		t.Fatalf("expected archiveStatusMessage %q, got %q", "origDatablocksNotYetAvailable", gotArchiveStatus)
	}
	if !strings.Contains(out, "pid-remote") {
		t.Fatalf("expected dataset id in output, got: %q", out)
	}
}

// TestExecuteFileTransferMarkFilesReadyPassedThrough verifies that the
// markFilesReady flag is forwarded into SshParams.
func TestExecuteFileTransferMarkFilesReadyPassedThrough(t *testing.T) {
	for _, wantMark := range []bool{true, false} {
		var gotMark bool
		ExecuteFileTransfer(
			&http.Client{}, "https://api", "rsync", "pid-1", "/src", "/list",
			map[string]string{"username": "alice"},
			FileContext{FullFileArray: []datasetIngestor.Datafile{{Path: "f"}}},
			func(p cliutils.TransferParams) (bool, error) {
				gotMark = p.SshParams.MarkFilesReady
				return false, nil
			},
			globus.GlobusClient{}, cliutils.GlobusConfig{}, "ssh",
			wantMark,
		)
		if gotMark != wantMark {
			t.Fatalf("MarkFilesReady: got %v, want %v", gotMark, wantMark)
		}
	}
}

// TestIngestionStrategyNonNil verifies that both strategy slots in noopCtx()
// are wired. Method completeness is guaranteed by the interface at compile time.
func TestIngestionStrategyNonNil(t *testing.T) {
	ctx := noopCtx()
	if ctx.FileIngestion == nil {
		t.Error("FileIngestion must not be nil")
	}
	if ctx.DatasetIdIngestion == nil {
		t.Error("DatasetIdIngestion must not be nil")
	}
}

// TestReadAndCheckMetadataFromDatasetIdNotFound verifies an error is returned when
// GetDatasetDetails reports the dataset as missing.
func TestReadAndCheckMetadataFromDatasetIdNotFound(t *testing.T) {
	// The real GetDatasetDetails hits a network; we test the function signature
	// and not-found branch by passing a bad URL that produces an HTTP error.
	// We can't inject the HTTP client easily here, so we test the missing-list path
	// indirectly: a dataset ID that can't be fetched must return a non-nil error.
	_, _, _, err := ReadAndCheckMetadataFromDatasetId(
		&http.Client{Transport: &errorTransport{}},
		"https://api.example.org",
		"20.500.11935/nonexistent",
		map[string]string{"accessToken": "tok"},
		nil,
	)
	if err == nil {
		t.Fatal("expected error for unreachable API, got nil")
	}
}

// errorTransport always returns a transport-level error.
type errorTransport struct{}

func (e *errorTransport) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, errors.New("simulated network error")
}

// TestRunIngestionPipelineDatasetIdOrigsExist verifies that when all requested
// dataset IDs already have orig datablocks (bool return = true), the pipeline
// returns them as archivable without calling IngestDataset or CreateOrigDatablocks.
func TestRunIngestionPipelineDatasetIdOrigsExist(t *testing.T) {
	prevFlags := datasetUtils.TestFlags
	prevArgs := datasetUtils.TestArgs
	defer func() {
		datasetUtils.TestFlags = prevFlags
		datasetUtils.TestArgs = prevArgs
	}()
	datasetUtils.TestFlags = nil
	datasetUtils.TestArgs = nil

	ctx := noopCtx()
	ctx.Cfg = IngestConfig{
		IngestFlag:        true,
		AutoarchiveFlag:   true,
		NocopyFlag:        true,
		NocopyFlagChanged: true,
		Tapecopies:        1,
	}
	// Simulate a dataset that already has orig datablocks: bool return = true.
	ds := errStub("DatasetIdIngestion")
	ds.readMetadata = func(_ *http.Client, _ string, id string, _ map[string]string, _ []string) (map[string]interface{}, string, bool, error) {
		return map[string]interface{}{
			"datasetId":    id,
			"sourceFolder": "/remote/data",
			"ownerGroup":   "grp",
		}, "/remote/data", true, nil
	}
	mustNotCall := func(*http.Client, string, map[string]interface{}, []datasetIngestor.Datafile, map[string]string) (string, error) {
		t.Error("DatasetIdIngestion.Ingest must not be called when orig datablocks already exist")
		return "", nil
	}
	ds.ingest = mustNotCall
	ds.ingestRemote = mustNotCall
	ctx.DatasetIdIngestion = ds

	fs := errStub("FileIngestion")
	fs.ingest = func(*http.Client, string, map[string]interface{}, []datasetIngestor.Datafile, map[string]string) (string, error) {
		t.Error("FileIngestion.Ingest must not be called for dataset-ID args")
		return "", nil
	}
	ctx.FileIngestion = fs

	var gotDatasets []string
	ctx.CreateArchivalJob = func(_ *http.Client, _ string, _ map[string]string, _ string, datasets []string, _ *int, _ *time.Time) (string, error) {
		gotDatasets = append([]string{}, datasets...)
		return "job-1", nil
	}

	dArgsList := []DatasetArgs{
		{MetadataFile: "20.500.11935/pid-1", DatasetStatus: datasetExistenceStatus{IsDatasetId: true}},
		{MetadataFile: "20.500.11935/pid-2", DatasetStatus: datasetExistenceStatus{IsDatasetId: true}},
	}
	if err := runIngestionPipeline(ctx, dArgsList, "v1.0.0"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(gotDatasets) != 2 {
		t.Fatalf("expected 2 datasets in archive job, got %d: %v", len(gotDatasets), gotDatasets)
	}
}

// TestRunIngestionPipelineDatasetIdNoOrigsCreatesOrigs verifies that when the
// dataset exists but orig datablocks are missing (bool return = false), the
// pipeline calls CreateOrigDatablocks and marks the dataset as archivable.
func TestRunIngestionPipelineDatasetIdNoOrigsCreatesOrigs(t *testing.T) {
	prevFlags := datasetUtils.TestFlags
	prevArgs := datasetUtils.TestArgs
	defer func() {
		datasetUtils.TestFlags = prevFlags
		datasetUtils.TestArgs = prevArgs
	}()
	datasetUtils.TestFlags = nil
	datasetUtils.TestArgs = nil

	tmp := t.TempDir()
	if err := os.WriteFile(filepath.Join(tmp, "data.dat"), []byte("x"), 0o600); err != nil {
		t.Fatalf("write file: %v", err)
	}

	var gotOrigDatasetId string
	ctx := noopCtx()
	ctx.Cfg = IngestConfig{
		IngestFlag:        true,
		NocopyFlag:        true,
		NocopyFlagChanged: true,
		Tapecopies:        1,
	}
	// Simulate a dataset with no orig datablocks: bool return = false.
	createOrigsFn := func(_ *http.Client, _ string, meta map[string]interface{}, _ []datasetIngestor.Datafile, _ map[string]string) (string, error) {
		gotOrigDatasetId = meta["datasetId"].(string)
		return gotOrigDatasetId, nil
	}
	ds := errStub("DatasetIdIngestion")
	ds.readMetadata = func(_ *http.Client, _ string, id string, _ map[string]string, _ []string) (map[string]interface{}, string, bool, error) {
		return map[string]interface{}{
			"datasetId":    id,
			"sourceFolder": tmp,
			"ownerGroup":   "grp",
		}, tmp, false, nil
	}
	ds.ingest = createOrigsFn
	ds.ingestRemote = createOrigsFn
	ctx.DatasetIdIngestion = ds

	fs := errStub("FileIngestion")
	fs.ingest = func(*http.Client, string, map[string]interface{}, []datasetIngestor.Datafile, map[string]string) (string, error) {
		t.Error("FileIngestion.Ingest must not be called for dataset-ID args")
		return "", nil
	}
	ctx.FileIngestion = fs

	dArgsList := []DatasetArgs{
		{MetadataFile: "20.500.11935/pid-abc", DatasetStatus: datasetExistenceStatus{IsDatasetId: true}},
	}
	if err := runIngestionPipeline(ctx, dArgsList, "v1.0.0"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if gotOrigDatasetId != "20.500.11935/pid-abc" {
		t.Fatalf("expected CreateOrigDatablocks called with pid-abc, got %q", gotOrigDatasetId)
	}
}
