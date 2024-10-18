package cliutils

import "testing"

func TestConvertToTransferTypeSsh(t *testing.T) {
	transfer := "ssh"
	testSsh, err := ConvertToTransferType(transfer)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if testSsh != Ssh {
		t.Errorf("Invalid transfer type: %v", testSsh)
	}
}

func TestConvertToTransferTypeGlobus(t *testing.T) {
	transfer := "globus"
	testSsh, err := ConvertToTransferType(transfer)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if testSsh != Globus {
		t.Errorf("Invalid transfer type: %v", testSsh)
	}
}

func TestConvertToTransferWrongType(t *testing.T) {
	transfer := "lolrandom"
	testSsh, err := ConvertToTransferType(transfer)
	if err == nil {
		t.Errorf("Expected an error. Got: %v - %v", testSsh, err)
	}
}
