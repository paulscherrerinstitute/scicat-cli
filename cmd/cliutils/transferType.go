package cliutils

import (
	"fmt"
	"strings"
)

type TransferType int

const (
	Ssh TransferType = iota
	Globus
	S3
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
	}
	return Ssh, fmt.Errorf("invalid transfer type was given: %s", input)
}
