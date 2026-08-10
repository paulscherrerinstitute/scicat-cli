package cliutils

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"path/filepath"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/paulscherrerinstitute/scicat-cli/v3/datasetIngestor"
)

// s3Transfer holds dependencies of transferFiles, so that they can be swapped with mocks in tests
type s3Transfer struct {
	upload         func(ctx context.Context, client *http.Client, brokerServer, bucket, datasetId, accessToken string, fileList []string, sourceFolder string) error
	markFilesReady func(client *http.Client, APIServer string, datasetId string, user map[string]string) error
}

// TransferFilesS3 sets up s3Transfer with real implementations of upload and markFilesReady
func TransferFilesS3(params TransferParams) (archivable bool, err error) {
	s := s3Transfer{upload: upload, markFilesReady: datasetIngestor.MarkFilesReady}
	return s.transferFiles(params)
}

// transferFiles uploads the dataset's files to S3, and on success marks the dataset as archivable.
func (s *s3Transfer) transferFiles(params TransferParams) (archivable bool, err error) {
	ctx := context.Background()
	err = s.upload(ctx, params.Client, params.BrokerServer, params.UploadBucket, params.DatasetId, params.User["accessToken"], params.Filelist, params.DatasetSourceFolder)
	if err == nil {
		log.Println("Marking files ready")
		err = s.markFilesReady(params.Client, params.ApiServer, params.DatasetId, params.User)
		if err != nil {
			log.Println("Failed to mark files ready i.e. dataset as archivable: ", err)
			return false, err
		}
		return true, nil
	}
	return false, err
}

// upload uploads contents of the sourceFolder, filtered by fileList, to bucket.
// The contents are uploaded under /datasetId prefix
// It uses brokerServer to get short-term credentials against user's accessToken
func upload(ctx context.Context, client *http.Client, brokerServer, bucket, datasetId, accessToken string, fileList []string, sourceFolder string) error {
	transferManagerClient, err := getTransferManagerClient(ctx, client, brokerServer, datasetId, accessToken)
	if err != nil {
		return err
	}
	return transferDirectory(ctx, transferManagerClient, bucket, fileList, sourceFolder, datasetId)
}

// s3BrokerCredsProvider implements the aws.CredentialsProvider interface
type s3BrokerCredsProvider struct {
	client       *http.Client
	brokerServer string
	datasetId    string
	operation    string
	accessToken  string

	getShortTermCreds func(client *http.Client, brokerServer string, datasetId string, operation string, accessToken string) (s3Creds, error)
}

func (s *s3BrokerCredsProvider) Retrieve(ctx context.Context) (aws.Credentials, error) {
	s3Creds, err := s.getShortTermCreds(s.client, s.brokerServer, s.datasetId, s.operation, s.accessToken)
	if err != nil {
		return aws.Credentials{}, err
	}
	expires, err := time.Parse(time.RFC3339, s3Creds.ExpiryTime)
	if err != nil {
		return aws.Credentials{}, err
	}
	return aws.Credentials{
		AccessKeyID:     s3Creds.AccessKey,
		SecretAccessKey: s3Creds.SecretKey,
		SessionToken:    s3Creds.SessionToken,
		Expires:         expires,
	}, nil
}

// getTransferManagerClient constructs a transfermanager client using s3BrokerCredsProvider which
// ensures credentials are auto refreshed on expiry
func getTransferManagerClient(ctx context.Context, client *http.Client, brokerServer, datasetId, accessToken string) (*transfermanager.Client, error) {
	s3bCredsProvider := &s3BrokerCredsProvider{
		client:       client,
		brokerServer: brokerServer,
		datasetId:    datasetId,
		operation:    "write",
		accessToken:  accessToken,

		getShortTermCreds: getShortTermCreds,
	}
	if s3bCredsProvider.accessToken == "" {
		return nil, fmt.Errorf("No access token")
	}
	cfg, err := config.LoadDefaultConfig(ctx, config.WithBaseEndpoint(CSCS_CEPH_ENDPOINT),
		config.WithRegion(CSCS_CEPH_AWS_REGION),
		config.WithCredentialsProvider(s3bCredsProvider))
	if err != nil {
		return nil, err
	}
	s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) { o.UsePathStyle = true })
	transferManagerClient := transfermanager.New(s3Client)
	return transferManagerClient, nil
}

// transferManagerAPI is the subset of transfermanager.Client's API that transferDirectory needs.
type transferManagerAPI interface {
	UploadDirectory(ctx context.Context, input *transfermanager.UploadDirectoryInput, opts ...func(*transfermanager.Options)) (*transfermanager.UploadDirectoryOutput, error)
}

type fileListingFilter struct {
	fileListing map[string]struct{}
}

func (f fileListingFilter) FilterFile(p string) bool {
	_, ok := f.fileListing[filepath.ToSlash(p)]
	return ok
}

func transferDirectory(ctx context.Context, client transferManagerAPI, bucket string, fileList []string, sourceFolder, datasetId string) error {
	// in case we're on Windows, convert sourceFolder ToSlash to be a s3 compatible prefix
	sourceFolder = filepath.ToSlash(sourceFolder)
	prefix := datasetId + sourceFolder

	input := &transfermanager.UploadDirectoryInput{
		Source:    &sourceFolder,
		Bucket:    &bucket,
		KeyPrefix: &prefix,
		Recursive: aws.Bool(true),
		Filter:    fileListingFilter{fileListing: makeAbsFilePathSet(sourceFolder, fileList)},
	}
	output, err := client.UploadDirectory(ctx, input)
	if err == nil {
		log.Printf("Uploaded %v objects, failed %v objects\n", output.ObjectsUploaded, output.ObjectsFailed)
	}
	return err
}

// create a set from fileList, transforming to (linux style) absolute paths
func makeAbsFilePathSet(sourceFolder string, fileList []string) map[string]struct{} {
	fileListing := make(map[string]struct{}, len(fileList))
	for _, f := range fileList {
		absPath := filepath.ToSlash(filepath.Join(sourceFolder, f))
		fileListing[absPath] = struct{}{}
	}
	return fileListing
}

type s3Creds struct {
	AccessKey    string `json:"access_key"`
	SecretKey    string `json:"secret_access_key"`
	SessionToken string `json:"session_token"`
	ExpiryTime   string `json:"expiry_time"`
}

// getShortTermCreds makes a GET request to brokerServer's /datasets/s3-creds endpoint with relevant params
func getShortTermCreds(client *http.Client, brokerServer string, datasetId string, operation string, accessToken string) (s3Creds, error) {
	u, err := url.Parse(brokerServer + "/datasets/s3-creds")
	if err != nil {
		return s3Creds{}, err
	}
	u.RawQuery = url.Values{"pid": {datasetId}, "operation": {operation}}.Encode()

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return s3Creds{}, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Authorization", "Bearer "+accessToken)

	resp, err := client.Do(req)
	if err != nil {
		return s3Creds{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return s3Creds{}, fmt.Errorf("failed to get short term S3 credentials, status code: %d", resp.StatusCode)
	}

	var creds s3Creds
	if err := json.NewDecoder(resp.Body).Decode(&creds); err != nil {
		return s3Creds{}, err
	}

	return creds, nil
}
