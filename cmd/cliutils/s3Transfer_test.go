package cliutils

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
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

// errCredsRetrieval is returned by shortTermCredsRetrieverMock when throwErr is set
var errCredsRetrieval = errors.New("getShortTermCreds failed to retrieve creds from brokerServer")

type shortTermCredsRetrieverMock struct {
	throwErr bool
	result   s3Creds
}

func (s *shortTermCredsRetrieverMock) getShortTermCreds(_ *http.Client, _ string, _ string, _ string, _ string) (s3Creds, error) {
	if s.throwErr {
		return s3Creds{}, errCredsRetrieval
	}
	return s.result, nil
}

// tests that Retrieve maps the s3Creds returned by getShortTermCreds onto aws.Credentials,
// and propagates retrieval and expiry parsing errors
func TestRetrieve(t *testing.T) {
	mockCreds := s3Creds{AccessKey: "test-access-key", SecretKey: "test-secret-key", SessionToken: "blah", ExpiryTime: "2026-12-01T12:23:34Z"}
	wantExpires, err := time.Parse(time.RFC3339, mockCreds.ExpiryTime)
	if err != nil {
		t.Fatalf("unable to parse timestamp %q: %v", mockCreds.ExpiryTime, err)
	}
	wantCreds := aws.Credentials{
		AccessKeyID:     mockCreds.AccessKey,
		SecretAccessKey: mockCreds.SecretKey,
		SessionToken:    mockCreds.SessionToken,
		Expires:         wantExpires,
	}

	// the same creds, but with an expiry time that time.Parse rejects
	badExpiryCreds := mockCreds
	badExpiryCreds.ExpiryTime = "not-a-timestamp"

	tests := []struct {
		name      string
		throwErr  bool
		creds     s3Creds
		wantCreds aws.Credentials // only checked when no error is expected
		wantErr   bool
		wantErrIs error
	}{
		{
			name:      "success",
			creds:     mockCreds,
			wantCreds: wantCreds,
		},
		{
			name:      "getShortTermCreds fails",
			throwErr:  true,
			wantErr:   true,
			wantErrIs: errCredsRetrieval,
		},
		{
			name:    "unparsable expiry time",
			creds:   badExpiryCreds,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := shortTermCredsRetrieverMock{throwErr: tt.throwErr, result: tt.creds}
			c := s3BrokerCredsProvider{
				getShortTermCreds: s.getShortTermCreds,
			}

			got, err := c.Retrieve(context.TODO())

			if tt.wantErr {
				if err == nil {
					t.Fatal("expected an error, got nil")
				}
				if tt.wantErrIs != nil && !errors.Is(err, tt.wantErrIs) {
					t.Fatalf("err = %v, want %v", err, tt.wantErrIs)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.wantCreds {
				t.Errorf("got aws.Credentials %v, want %v", got, tt.wantCreds)
			}
		})
	}
}

// tests that getShortTermCreds makes the expected request against brokerServer's
// /datasets/s3-creds endpoint, and decodes its response into s3Creds
func TestGetShortTermCreds(t *testing.T) {
	const (
		datasetId   = "20.500.11935/abc-123"
		operation   = "write"
		accessToken = "testToken"
	)
	credsJSON := `{"access_key":"test-access-key","secret_access_key":"test-secret-key","session_token":"blah","expiry_time":"2026-12-01T12:23:34Z"}`
	wantCreds := s3Creds{AccessKey: "test-access-key", SecretKey: "test-secret-key", SessionToken: "blah", ExpiryTime: "2026-12-01T12:23:34Z"}

	tests := []struct {
		name         string
		status       int
		body         string
		brokerServer string // overrides server.URL to test for URL parse failure
		wantCreds    s3Creds
		wantErr      bool
	}{
		{
			name:      "success",
			status:    http.StatusOK,
			body:      credsJSON,
			wantCreds: wantCreds,
		},
		{
			name:    "non-OK status",
			status:  http.StatusForbidden,
			body:    `{"message":"not allowed to write this dataset"}`,
			wantErr: true,
		},
		{
			name:    "malformed response body",
			status:  http.StatusOK,
			body:    `{"access_key":`,
			wantErr: true,
		},
		{
			name:         "unparsable brokerServer URL",
			brokerServer: "not-a-url",
			wantErr:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
				if req.Method != http.MethodGet {
					t.Errorf("method = %q, want %q", req.Method, http.MethodGet)
				}
				if got := req.URL.Path; got != "/datasets/s3-creds" {
					t.Errorf("path = %q, want %q", got, "/datasets/s3-creds")
				}
				if got := req.URL.Query().Get("pid"); got != datasetId {
					t.Errorf("pid = %q, want %q", got, datasetId)
				}
				if got := req.URL.Query().Get("operation"); got != operation {
					t.Errorf("operation = %q, want %q", got, operation)
				}
				if got := req.Header.Get("Accept"); got != "application/json" {
					t.Errorf("Accept header = %q, want %q", got, "application/json")
				}
				if got := req.Header.Get("Authorization"); got != "Bearer "+accessToken {
					t.Errorf("Authorization header = %q, want %q", got, "Bearer "+accessToken)
				}

				rw.WriteHeader(tt.status)
				rw.Write([]byte(tt.body))
			}))
			defer server.Close()

			brokerServer := server.URL
			if tt.brokerServer != "" {
				brokerServer = tt.brokerServer
			}

			got, err := getShortTermCreds(&http.Client{}, brokerServer, datasetId, operation, accessToken)

			if tt.wantErr {
				if err == nil {
					t.Fatal("expected an error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.wantCreds {
				t.Errorf("got s3Creds %v, want %v", got, tt.wantCreds)
			}
		})
	}
}
