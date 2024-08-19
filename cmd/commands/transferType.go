package cmd

import (
	"fmt"
	"strings"
)

type transferType int

const (
	Ssh transferType = iota
	Globus
)

func convertToTransferType(input string) (transferType, error) {
	input = strings.ToLower(input)
	switch input {
	case "ssh":
		return Ssh, nil
	case "globus":
		return Globus, nil
	}
	return Ssh, fmt.Errorf("invalid transfer type was given: %s", input)
}
