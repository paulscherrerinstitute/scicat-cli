package cliutils

import (
	"os"
	"testing"
)

func TestS3TransferManual(t *testing.T) {
	accessToken := os.Getenv("S3_BROKER_TOKEN")
	if accessToken == "" {
		t.Skip("set S3_BROKER_TOKEN to run this test against a real S3 broker")
	}

	datasetId := os.Getenv("S3_BROKER_DATASET_ID")
	if datasetId == "" {
		datasetId = "20.500.11935/514086cf-7ca5-422d-9e7b-32e6720b9ca4"
	}

	params := TransferParams{
		SshParams: SshParams{
			User:      map[string]string{"accessToken": accessToken},
			ApiServer: DEV_API_SERVER,
		},
		DatasetId:           datasetId,
		DatasetSourceFolder: "/home/zade_o/Downloads/test-dataset-12",
	}

	archivable, err := S3Transfer(params)
	if err != nil {
		t.Fatalf("S3Transfer returned an error: %v", err)
	}

	t.Logf("archivable: %v", archivable)
}
