package cliutils

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
)

// mockS3Uploader is a receiver struct implementing the `upload` dependency of TransferFilesS3
type mockS3Uploader struct {
	uploadErr error
}

func (f *mockS3Uploader) upload(ctx context.Context, client *http.Client, brokerServer, bucket, datasetId, accessToken string, fileList []string, sourceFolder string) error {
	return f.uploadErr
}

// mockDatasetIngestor is a receiver struct implementing the `MarkFilesReady` dependency of TransferFilesS3
type mockDatasetIngestor struct {
	err    error
	called bool
}

func (f *mockDatasetIngestor) MarkFilesReady(client *http.Client, APIServer string, datasetId string, user map[string]string) error {
	f.called = true
	return f.err
}

// tests that the exported function TransferFilesS3 returns expected combination of archivable and error
// for all (mocked) behaviors of upload and MarkFilesReady
func TestTransferFilesS3_ControlFlow(t *testing.T) {
	wantErr := errors.New("test")

	tests := []struct {
		name              string
		uploadErr         error
		markReadyErr      error
		wantArchivable    bool
		wantErr           error
		wantMarkReadyCall bool
	}{
		{
			name:      "upload fails",
			uploadErr: wantErr,
			wantErr:   wantErr,
		},
		{
			name:              "MarkFilesReady fails",
			markReadyErr:      wantErr,
			wantErr:           wantErr,
			wantMarkReadyCall: true,
		},
		{
			name:              "success",
			wantArchivable:    true,
			wantMarkReadyCall: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			deps := &mockS3Uploader{uploadErr: tt.uploadErr}
			ingestor := &mockDatasetIngestor{err: tt.markReadyErr}
			s := s3Transfer{upload: deps.upload, markFilesReady: ingestor.MarkFilesReady}

			archivable, err := s.transferFiles(TransferParams{})

			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("err = %v, want %v", err, tt.wantErr)
				}
			} else if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if archivable != tt.wantArchivable {
				t.Errorf("archivable = %v, want %v", archivable, tt.wantArchivable)
			}
			if ingestor.called != tt.wantMarkReadyCall {
				t.Errorf("MarkFilesReady called = %v, want %v", ingestor.called, tt.wantMarkReadyCall)
			}
		})
	}
}

// mockTransferManagerClient implements the transferManagerAPI interface
type mockTransferManagerClient struct {
	gotInput *transfermanager.UploadDirectoryInput
	err      error
}

func (f *mockTransferManagerClient) UploadDirectory(ctx context.Context, input *transfermanager.UploadDirectoryInput, opts ...func(*transfermanager.Options)) (*transfermanager.UploadDirectoryOutput, error) {
	f.gotInput = input
	if f.err != nil {
		return nil, f.err
	}
	return &transfermanager.UploadDirectoryOutput{}, nil
}

// tests that the mockTransferManagerClient was called with expected inputs
// including the FileFilter callback
func TestTransferDirectory_BuildsExpectedInput(t *testing.T) {
	client := &mockTransferManagerClient{}

	sourceFolder := "/data/raw"
	fileList := []string{"a.txt", "sub/b.txt"}
	bucket := "my-bucket"
	datasetId := "20.500.11935/abc-123"

	err := transferDirectory(context.Background(), client, bucket, fileList, sourceFolder, datasetId)
	if err != nil {
		t.Fatalf("transferDirectory returned an error: %v", err)
	}
	if client.gotInput == nil {
		t.Fatal("UploadDirectory was never called")
	}

	if got := *client.gotInput.Bucket; got != bucket {
		t.Errorf("Bucket = %q, want %q", got, bucket)
	}
	if got := *client.gotInput.Source; got != sourceFolder {
		t.Errorf("Source = %q, want %q", got, sourceFolder)
	}
	if wantPrefix := datasetId + sourceFolder; *client.gotInput.KeyPrefix != wantPrefix {
		t.Errorf("KeyPrefix = %q, want %q", *client.gotInput.KeyPrefix, wantPrefix)
	}
	if client.gotInput.Recursive == nil || !*client.gotInput.Recursive {
		t.Error("Recursive = false, want true")
	}

	filter, ok := client.gotInput.Filter.(fileListingFilter)
	if !ok {
		t.Fatalf("Filter has unexpected type %T", client.gotInput.Filter)
	}
	if !filter.FilterFile(sourceFolder + "/a.txt") {
		t.Error("expected a.txt to pass the filter")
	}
	if !filter.FilterFile(sourceFolder + "/sub/b.txt") {
		t.Error("expected sub/b.txt to pass the filter")
	}
	if filter.FilterFile(sourceFolder + "/not-listed.txt") {
		t.Error("expected not-listed.txt to be filtered out")
	}
}

func TestTransferDirectory_PropagatesUploadError(t *testing.T) {
	wantErr := errors.New("test")
	client := &mockTransferManagerClient{err: wantErr}

	err := transferDirectory(context.Background(), client, "bucket", nil, "/data", "dataset")
	if !errors.Is(err, wantErr) {
		t.Fatalf("transferDirectory error = %v, want %v", err, wantErr)
	}
}
