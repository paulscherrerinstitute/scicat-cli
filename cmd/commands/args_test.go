package cmd

import (
	"testing"

	"github.com/paulscherrerinstitute/scicat-cli/v3/datasetUtils"
)

func TestArgs(t *testing.T) {
	// test cases
	tests := []struct {
		name         string
		expectedArgs []interface{}
		args         []string
	}{
		// datasetIngestor
		{
			name: "datasetIngestor test with 1 param",
			expectedArgs: []interface{}{
				"some/place/metadata.json",
				"",
				"",
			},
			args: []string{"datasetIngestor", "some/place/metadata.json"},
		},
		{
			name: "datasetIngestor test with 2 params - folderlisting.txt",
			expectedArgs: []interface{}{
				"some/place/metadata.json",
				"",
				"folderlisting.txt",
			},
			args: []string{"datasetIngestor", "some/place/metadata.json", "folderlisting.txt"},
		},
		{
			name: "datasetIngestor test with 2 params - filelisting",
			expectedArgs: []interface{}{
				"some/place/metadata.json",
				"/some/path/",
				"",
			},
			args: []string{"datasetIngestor", "some/place/metadata.json", "/some/path/"},
		},
	}

	// running test cases
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			//flag.CommandLine = flag.NewFlagSet(test.name, flag.ExitOnError)
			datasetUtils.TestArgs = func(args []interface{}) {
				passing := true
				for i := range test.expectedArgs {
					if test.expectedArgs[i] != args[i] {
						t.Logf("'%v' is not correct, expected: '%s'", args[i], test.expectedArgs[i])
						passing = false
					}
				}
				if !passing {
					t.Fail()
				}
			}

			rootCmd.SetArgs(test.args)
			Execute()
		})
	}
}
