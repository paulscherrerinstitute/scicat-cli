package backend

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/paulscherrerinstitute/scicat-cli/v3/cmd/cliutils"
	"github.com/paulscherrerinstitute/scicat-cli/v3/datasetIngestor"
)

func TestNewFileService(t *testing.T) {
	user := map[string]string{"username": "alice"}
	fs := NewFileService(user, "/data/source", "folders.txt", "/tmp/files.txt", "rsync.example.org")

	if fs.User["username"] != "alice" {
		t.Fatalf("expected user to be set")
	}
	if fs.MetadataSourceFolder != "/data/source" {
		t.Fatalf("unexpected metadata source folder: %s", fs.MetadataSourceFolder)
	}
	if fs.FolderListingTxt != "folders.txt" {
		t.Fatalf("unexpected folder listing path: %s", fs.FolderListingTxt)
	}
	if fs.AbsFilelistPath != "/tmp/files.txt" {
		t.Fatalf("unexpected abs file list path: %s", fs.AbsFilelistPath)
	}
	if fs.RsyncServer != "rsync.example.org" {
		t.Fatalf("unexpected rsync server: %s", fs.RsyncServer)
	}
}

func TestEvaluateSymlinkStrategy(t *testing.T) {
	fs := &FileService{}

	if got := fs.EvaluateSymlinkStrategy(false, "keep"); got != "" {
		t.Fatalf("expected empty strategy when unchanged, got %q", got)
	}
	if got := fs.EvaluateSymlinkStrategy(true, "delete"); got != "sA" {
		t.Fatalf("expected sA, got %q", got)
	}
	if got := fs.EvaluateSymlinkStrategy(true, "keep"); got != "kA" {
		t.Fatalf("expected kA, got %q", got)
	}
	if got := fs.EvaluateSymlinkStrategy(true, "keepInternalOnly"); got != "dA" {
		t.Fatalf("expected dA, got %q", got)
	}
}

func TestResetLocalSymlinkStrategy(t *testing.T) {
	fs := &FileService{}

	if got := fs.ResetLocalSymlinkStrategy("sA"); got != "sA" {
		t.Fatalf("expected sA to remain unchanged")
	}
	if got := fs.ResetLocalSymlinkStrategy("kA"); got != "kA" {
		t.Fatalf("expected kA to remain unchanged")
	}
	if got := fs.ResetLocalSymlinkStrategy("dA"); got != "dA" {
		t.Fatalf("expected dA to remain unchanged")
	}
	if got := fs.ResetLocalSymlinkStrategy("k"); got != "" {
		t.Fatalf("expected non-global strategy to reset to empty, got %q", got)
	}
}

func TestGetFilenameFilterCallback(t *testing.T) {
	fs := &FileService{}
	filter := fs.GetFilenameFilterCallback()

	if keep := filter("valid/path.txt"); !keep {
		t.Fatalf("expected valid file to be kept")
	}
	if fs.TotalIllegalFileNames != 0 {
		t.Fatalf("unexpected illegal counter after valid path")
	}

	if keep := filter("bad*file.txt"); keep {
		t.Fatalf("expected path with illegal char to be rejected")
	}
	if fs.TotalIllegalFileNames != 1 {
		t.Fatalf("expected illegal counter 1, got %d", fs.TotalIllegalFileNames)
	}

	if keep := filter("bad   name.txt"); keep {
		t.Fatalf("expected path with triple spaces to be rejected")
	}
	if fs.TotalIllegalFileNames != 2 {
		t.Fatalf("expected illegal counter 2, got %d", fs.TotalIllegalFileNames)
	}
}

func TestResolveDatasetPathsWithoutListing(t *testing.T) {
	fs := &FileService{MetadataSourceFolder: "/data/one"}
	paths := fs.ResolveDatasetPaths()

	if len(paths) != 1 || paths[0] != "/data/one" {
		t.Fatalf("expected one path from metadata source, got %#v", paths)
	}
}

func TestResolveDatasetPathsWithListing(t *testing.T) {
	tmpDir := t.TempDir()
	listPath := filepath.Join(tmpDir, "folderlisting.txt")
	content := "\n# comment\n  /data/a  \n/data/b\n"
	if err := os.WriteFile(listPath, []byte(content), 0o600); err != nil {
		t.Fatalf("failed to create listing file: %v", err)
	}

	fs := &FileService{FolderListingTxt: listPath}
	paths := fs.ResolveDatasetPaths()

	if len(paths) != 2 {
		t.Fatalf("expected 2 dataset paths, got %d (%#v)", len(paths), paths)
	}
	if paths[0] != "/data/a" || paths[1] != "/data/b" {
		t.Fatalf("unexpected dataset paths: %#v", paths)
	}
}

func TestTransferDatasetFilesRequiresStrategy(t *testing.T) {
	fs := &FileService{}
	_, err := fs.TransferDatasetFiles("pid", "/data/source", nil, nil, "")
	if err == nil {
		t.Fatalf("expected error when transfer strategy is not initialized")
	}
}

func TestTransferDatasetFilesSSH(t *testing.T) {
	fs := &FileService{
		User:               map[string]string{"username": "alice"},
		RsyncServer:        "rsync.psi.ch",
		AbsFilelistPath:    "/tmp/filelist.txt",
		activeTransferType: cliutils.Ssh,
	}

	called := false
	fs.TransferFiles = func(params cliutils.TransferParams) (bool, error) {
		called = true
		if params.DatasetId != "pid-1" {
			t.Fatalf("unexpected dataset id: %s", params.DatasetId)
		}
		if params.DatasetSourceFolder != "/data/source" {
			t.Fatalf("unexpected source folder: %s", params.DatasetSourceFolder)
		}
		if params.SshParams.RsyncServer != "rsync.psi.ch" {
			t.Fatalf("unexpected rsync server: %s", params.SshParams.RsyncServer)
		}
		if params.SshParams.AbsFilelistPath != "/tmp/filelist.txt" {
			t.Fatalf("unexpected abs filelist path: %s", params.SshParams.AbsFilelistPath)
		}
		if params.SshParams.User["username"] != "alice" {
			t.Fatalf("expected ssh params user to be propagated")
		}
		return true, nil
	}

	archivable, err := fs.TransferDatasetFiles("pid-1", "/data/source", nil, nil, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called {
		t.Fatalf("expected transfer strategy to be called")
	}
	if !archivable {
		t.Fatalf("expected archivable=true from transfer callback")
	}
}

func TestTransferDatasetFilesGlobus(t *testing.T) {
	fs := &FileService{
		activeTransferType: cliutils.Globus,
		GlobusConfig: cliutils.GlobusConfig{
			SourceCollection:      "src-col",
			SourcePrefixPath:      "/src",
			DestinationCollection: "dst-col",
			DestinationPrefixPath: "/dst",
		},
	}

	files := []datasetIngestor.Datafile{
		{Path: "a/file1", IsSymlink: false},
		{Path: "b/link", IsSymlink: true},
	}

	fs.TransferFiles = func(params cliutils.TransferParams) (bool, error) {
		if len(params.GlobusParams.Filelist) != 2 {
			t.Fatalf("expected 2 globus files, got %d", len(params.GlobusParams.Filelist))
		}
		if params.GlobusParams.Filelist[0] != "a/file1" || params.GlobusParams.Filelist[1] != "b/link" {
			t.Fatalf("unexpected file list: %#v", params.GlobusParams.Filelist)
		}
		if len(params.GlobusParams.IsSymlinkList) != 2 {
			t.Fatalf("expected 2 globus symlink flags, got %d", len(params.GlobusParams.IsSymlinkList))
		}
		if params.GlobusParams.IsSymlinkList[0] || !params.GlobusParams.IsSymlinkList[1] {
			t.Fatalf("unexpected symlink flags: %#v", params.GlobusParams.IsSymlinkList)
		}
		if params.GlobusParams.SrcCollection != "src-col" || params.GlobusParams.DestCollection != "dst-col" {
			t.Fatalf("unexpected globus collections")
		}
		return false, nil
	}

	archivable, err := fs.TransferDatasetFiles("pid-2", "/data/source", files, nil, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if archivable {
		t.Fatalf("expected archivable=false from transfer callback")
	}
}

func TestScanAndVerifyFilesEmptyDataset(t *testing.T) {
	tmpDir := t.TempDir()
	fs := &FileService{}

	_, _, _, _, ok, err := fs.ScanAndVerifyFiles(tmpDir, "", func(string, string) (bool, error) {
		return true, nil
	})

	if err == nil {
		t.Fatalf("expected empty dataset error")
	}
	if ok {
		t.Fatalf("expected ok=false for empty dataset")
	}
	if fs.EmptyDatasetsCount != 1 {
		t.Fatalf("expected empty dataset counter to increment, got %d", fs.EmptyDatasetsCount)
	}
}

func TestScanAndVerifyFilesSuccess(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "data.txt")
	if err := os.WriteFile(filePath, []byte("payload"), 0o600); err != nil {
		t.Fatalf("failed to create dataset file: %v", err)
	}

	fs := &FileService{}
	files, start, end, owner, ok, err := fs.ScanAndVerifyFiles(tmpDir, "", func(string, string) (bool, error) {
		return true, nil
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Fatalf("expected ok=true")
	}
	if len(files) == 0 {
		t.Fatalf("expected at least one file in scan result")
	}
	if start == (time.Time{}) || end == (time.Time{}) {
		t.Fatalf("expected non-zero start/end times")
	}
	_ = owner
	if fs.EmptyDatasetsCount != 0 {
		t.Fatalf("expected empty counter to remain zero")
	}
}

func TestEvaluateSymlinkByStrategy(t *testing.T) {
	tmpDir := t.TempDir()
	sourceDir := filepath.Join(tmpDir, "source")
	if err := os.MkdirAll(sourceDir, 0o755); err != nil {
		t.Fatalf("failed to create source dir: %v", err)
	}
	sourceDirCanonical, err := filepath.EvalSymlinks(sourceDir)
	if err != nil {
		t.Fatalf("failed to canonicalize source dir: %v", err)
	}

	insideTarget := filepath.Join(sourceDir, "target.txt")
	if err := os.WriteFile(insideTarget, []byte("x"), 0o600); err != nil {
		t.Fatalf("failed to create target: %v", err)
	}

	insideLink := filepath.Join(sourceDir, "inside-link")
	if err := os.Symlink("target.txt", insideLink); err != nil {
		t.Fatalf("failed to create symlink: %v", err)
	}

	fs := &FileService{}

	keep, strategy := fs.EvaluateSymlink(insideLink, sourceDirCanonical, "kA", func(string, string) string { return "" })
	if !keep || strategy != "kA" {
		t.Fatalf("expected keep=true with kA strategy")
	}

	keep, strategy = fs.EvaluateSymlink(insideLink, sourceDirCanonical, "sA", func(string, string) string { return "" })
	if keep || strategy != "sA" {
		t.Fatalf("expected keep=false with sA strategy")
	}

	keep, strategy = fs.EvaluateSymlink(insideLink, sourceDirCanonical, "dA", func(string, string) string { return "" })
	if !keep || strategy != "dA" {
		t.Fatalf("expected keep=true for internal link with dA")
	}
}

func TestEvaluateSymlinkInteractiveDefault(t *testing.T) {
	tmpDir := t.TempDir()
	sourceDir := filepath.Join(tmpDir, "source")
	if err := os.MkdirAll(sourceDir, 0o755); err != nil {
		t.Fatalf("failed to create source dir: %v", err)
	}
	sourceDirCanonical, err := filepath.EvalSymlinks(sourceDir)
	if err != nil {
		t.Fatalf("failed to canonicalize source dir: %v", err)
	}

	target := filepath.Join(sourceDir, "target.txt")
	if err := os.WriteFile(target, []byte("x"), 0o600); err != nil {
		t.Fatalf("failed to create target: %v", err)
	}

	link := filepath.Join(sourceDir, "link")
	if err := os.Symlink("target.txt", link); err != nil {
		t.Fatalf("failed to create symlink: %v", err)
	}

	fs := &FileService{}
	keep, strategy := fs.EvaluateSymlink(link, sourceDirCanonical, "", func(string, string) string {
		return ""
	})

	if !keep {
		t.Fatalf("expected default strategy to keep internal link")
	}
	if strategy != "d" {
		t.Fatalf("expected empty input to default to d, got %q", strategy)
	}
}
