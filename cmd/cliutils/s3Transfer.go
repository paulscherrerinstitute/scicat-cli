package cliutils

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	smithyendpoints "github.com/aws/smithy-go/endpoints"
)

type S3Creds struct {
	AccessKey    string `json:"access_key"`
	SecretKey    string `json:"secret_access_key"`
	SessionToken string `json:"session_token"`
	ExpiryTime   string `json:"expiry_time"`
}

type SciCatS3CredsProvider struct {
	client       *http.Client
	brokerServer string
	datasetId    string
	operation    string
	accessToken  string
}

type CephEndpointResolver struct {
	bucket string
}

func (c *CephEndpointResolver) ResolveEndpoint(ctx context.Context, params s3.EndpointParameters) (
	smithyendpoints.Endpoint, error,
) {
	boolTrue := true
	params.ForcePathStyle = &boolTrue
	params.Bucket = &c.bucket
	return s3.NewDefaultEndpointResolverV2().ResolveEndpoint(ctx, params)
}

func (s *SciCatS3CredsProvider) Retrieve(ctx context.Context) (aws.Credentials, error) {
	s3Creds, err := getShortTermCreds(s.client, s.brokerServer, s.datasetId, s.operation, s.accessToken)
	expires, _ := time.Parse(time.RFC3339, s3Creds.ExpiryTime)
	return aws.Credentials{
		AccessKeyID:     s3Creds.AccessKey,
		SecretAccessKey: s3Creds.SecretKey,
		SessionToken:    s3Creds.SessionToken,
		Expires:         expires,
	}, err
}

func S3Transfer(params TransferParams) (archivable bool, err error) {
	ctx := context.TODO()
	s := &SciCatS3CredsProvider{
		client:       http.DefaultClient,
		brokerServer: "https://s3-broker.development.psi.ch",
		datasetId:    params.DatasetId,
		operation:    "write",
		accessToken:  params.SshParams.User["accessToken"],
	}
	if s.accessToken == "" {
		log.Fatalln("No access token")
	}
	cfg, err := config.LoadDefaultConfig(ctx, config.WithBaseEndpoint("https://rgw.cscs.ch"), config.WithRegion("us-east-1"), config.WithCredentialsProvider(s), config.WithClientLogMode(aws.LogResponseWithBody))
	bucket := "psi-upload-dev"
	c := &CephEndpointResolver{bucket: bucket}
	s3Client := s3.NewFromConfig(cfg, s3.WithEndpointResolverV2(c))
	prefix := params.DatasetId + "/"
	resp, err := s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: &bucket, Prefix: &prefix})
	if err != nil {
		log.Fatalln("Error calling list obj v2", err)
	}
	log.Printf("%v keys in the bucket", *resp.KeyCount)
	return true, nil
}

func getShortTermCreds(client *http.Client, brokerServer string, datasetId string, operation string, accessToken string) (S3Creds, error) {
	myurl := brokerServer + "/datasets/s3-creds?pid=" + url.QueryEscape(datasetId) + "&operation=" + url.QueryEscape(operation)

	req, err := http.NewRequest("GET", myurl, nil)
	if err != nil {
		return S3Creds{}, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Authorization", "Bearer "+accessToken)

	resp, err := client.Do(req)
	if err != nil {
		return S3Creds{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return S3Creds{}, fmt.Errorf("failed to get short term S3 credentials, status code: %d", resp.StatusCode)
	}

	var creds S3Creds
	if err := json.NewDecoder(resp.Body).Decode(&creds); err != nil {
		return S3Creds{}, err
	}

	return creds, nil
}
