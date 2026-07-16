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
		datasetId = "20.500.11935/d6ed9958-5930-4b0e-aed9-90d294201280"
	}

	params := TransferParams{
		SshParams: SshParams{
			User: map[string]string{"accessToken": accessToken},
		},
		DatasetId: datasetId,
	}

	archivable, err := S3Transfer(params)
	if err != nil {
		t.Fatalf("S3Transfer returned an error: %v", err)
	}

	t.Logf("archivable: %v", archivable)
}
