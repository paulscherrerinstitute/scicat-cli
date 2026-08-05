package datasetUtils

import (
	"encoding/json"
	"fmt"
	"strings"
)

type TransferType int

const (
	Ssh TransferType = iota
	Globus
	S3
	Rocrate
)

func ConvertToTransferType(input string) (TransferType, error) {
	input = strings.ToLower(input)
	switch input {
	case "ssh":
		return Ssh, nil
	case "globus":
		return Globus, nil
	case "s3":
		return S3, nil
	case "rocrate":
		return Rocrate, nil
	}
	return Ssh, fmt.Errorf("invalid transfer type was given: %s", input)
}

func (t TransferType) String() string {
	switch t {
	case Ssh:
		return "ssh"
	case Globus:
		return "globus"
	case S3:
		return "s3"
	case Rocrate:
		return "rocrate"
	}
	return ""
}

func (t TransferType) MarshalJSON() ([]byte, error) {
	return json.Marshal(t.String())
}
