package cliutils

import (
	"path/filepath"
	"strings"
)

type IngestPair struct {
	MetadataFile       string
	DatasetFileListTxt string
	FolderListingTxt   string
	AbsFileListing     string
}

// parseIngestArguments processes the CLI positional arguments and organizes them into JSON/TXT pairs.
// It retains full backward compatibility for legacy inputs:
//   - 1 arg:  [json]
//   - 2 args: [json, txt] OR [json, json] (detected via extension)
//
// It also handles multi-pair inputs using delimiters: [json:txt, json, json:txt]
func ParseIngestArguments(args []string) []IngestPair {
	var pairs []IngestPair
	hasDelimiter := false
	for _, arg := range args {
		if strings.Contains(arg, ":") {
			hasDelimiter = true
			break
		}
	}

	// Legacy mode: 1 or 2 raw args without delimiters
	if len(args) <= 2 && !hasDelimiter {
		if len(args) == 1 {
			pairs = append(pairs, IngestPair{MetadataFile: args[0]})
		} else if len(args) == 2 {
			// Distinguish between (config1.json list1.txt) and (config1.json config2.json)
			if strings.HasSuffix(args[1], ".json") {
				pairs = append(pairs, IngestPair{MetadataFile: args[0]})
				pairs = append(pairs, IngestPair{MetadataFile: args[1]})
			} else {
				pair := IngestPair{MetadataFile: args[0]}
				argFileName := filepath.Base(args[1])
				if argFileName == "folderlisting.txt" {
					pair.FolderListingTxt = args[1]
				} else {
					pair.DatasetFileListTxt = args[1]
					pair.AbsFileListing, _ = filepath.Abs(pair.DatasetFileListTxt)
				}
				pairs = append(pairs, pair)
			}
		}
	} else {
		// Multi-pair mode using delimiters
		for _, arg := range args {
			parts := strings.SplitN(arg, ":", 2)
			pair := IngestPair{MetadataFile: parts[0]}

			if len(parts) == 2 && parts[1] != "" {
				txtFile := parts[1]
				argFileName := filepath.Base(txtFile)
				if argFileName == "folderlisting.txt" {
					pair.FolderListingTxt = txtFile
				} else {
					pair.DatasetFileListTxt = txtFile
					pair.AbsFileListing, _ = filepath.Abs(pair.DatasetFileListTxt)
				}
			}
			pairs = append(pairs, pair)
		}
	}

	return pairs
}
