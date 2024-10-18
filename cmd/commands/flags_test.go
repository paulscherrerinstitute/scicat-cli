package cmd

import (
	"testing"

	"github.com/paulscherrerinstitute/scicat-cli/v3/datasetUtils"
	"github.com/spf13/pflag"
)

func TestMainFlags(t *testing.T) {
	// test cases
	tests := []struct {
		name  string
		flags map[string]interface{}
		args  []string
	}{
		// datasetArchiver
		{
			name: "datasetArchiver test without flags",
			flags: map[string]interface{}{
				"testenv":        false,
				"devenv":         false,
				"localenv":       false,
				"noninteractive": false,
				"version":        false,
				"user":           "",
				"token":          "",
				"tapecopies":     1,
				"ownergroup":     "a",
			},
			args: []string{"datasetArchiver", "--ownergroup", "a", "an argument placeholder"},
		},
		{
			name: "datasetArchiver test with all flags set",
			flags: map[string]interface{}{
				"testenv":        true,
				"devenv":         false,
				"localenv":       false,
				"noninteractive": true,
				"version":        true,
				"user":           "usertest:passtest",
				"token":          "token",
				"tapecopies":     6571579,
				"ownergroup":     "group1",
			},
			args: []string{
				"datasetArchiver",
				"--testenv",
				//"--devenv",
				//"--localenv",
				"--noninteractive",
				"--user",
				"usertest:passtest",
				"--token",
				"token",
				"--tapecopies",
				"6571579",
				"--version",
				"an argument placeholder",
				"--ownergroup",
				"group1",
			},
		},
		// datasetCleaner
		{
			name: "datasetCleaner test without flags",
			flags: map[string]interface{}{
				"testenv":           false,
				"devenv":            false,
				"nonInteractive":    false,
				"removeFromCatalog": false,
				"version":           false,
				"user":              "",
				"token":             "",
			},
			args: []string{"datasetCleaner", "argument placeholder"},
		},
		{
			name: "datasetCleaner test with all flags set",
			flags: map[string]interface{}{
				"testenv":           true,
				"devenv":            false,
				"nonInteractive":    true,
				"removeFromCatalog": true,
				"version":           true,
				"user":              "usertest:passtest",
				"token":             "token",
			},
			args: []string{
				"datasetCleaner",
				"--testenv",
				//"--devenv",
				"--nonInteractive",
				"--removeFromCatalog",
				"--user",
				"usertest:passtest",
				"--token",
				"token",
				"--version",
				"argument placeholder",
			},
		},
		// datasetGetProposal
		{
			name: "datasetGetProposal test without flags",
			flags: map[string]interface{}{
				"testenv": false,
				"devenv":  false,
				"version": false,
				"user":    "",
				"token":   "",
				"field":   "",
			},
			args: []string{"datasetGetProposal", "argument placeholder"},
		},
		{
			name: "datasetGetProposal test with all flags set",
			flags: map[string]interface{}{
				"testenv": true,
				"devenv":  false,
				"version": true,
				"user":    "usertest:passtest",
				"token":   "token",
				"field":   "some field",
			},
			args: []string{
				"datasetGetProposal",
				"--testenv",
				//"--devenv",
				"--user",
				"usertest:passtest",
				"--token",
				"token",
				"--field",
				"some field",
				"--version",
				"argument placeholder",
			},
		},
		// datasetIngestor
		{
			name: "datasetIngestor test without flags",
			flags: map[string]interface{}{
				"ingest":              false,
				"testenv":             false,
				"devenv":              false,
				"localenv":            false,
				"tunnelenv":           false,
				"noninteractive":      false,
				"copy":                false,
				"nocopy":              false,
				"autoarchive":         false,
				"allowexistingsource": false,
				"version":             false,
				"user":                "",
				"token":               "",
				"linkfiles":           "keepInternalOnly",
				"addattachment":       "",
				"addcaption":          "",
				"tapecopies":          0,
			},
			args: []string{"datasetIngestor", "argument placeholder"},
		},
		{ // note: the environment flags are mutually exclusive, not all of them can be set at once
			name: "datasetIngestor test with (almost) all flags set",
			flags: map[string]interface{}{
				"ingest":              true,
				"testenv":             true,
				"devenv":              false,
				"localenv":            false,
				"tunnelenv":           false,
				"noninteractive":      true,
				"copy":                true,
				"nocopy":              false,
				"autoarchive":         true,
				"allowexistingsource": true,
				"version":             true,
				"user":                "usertest:passtest",
				"token":               "token",
				"linkfiles":           "somerandomstring",
				"addattachment":       "random attachment string",
				"addcaption":          "a seemingly random caption",
				"tapecopies":          6571579,
			},
			args: []string{
				"datasetIngestor",
				"--ingest",
				"--testenv",
				//"--localenv",
				//"--tunnelenv",
				"--noninteractive",
				"--user",
				"usertest:passtest",
				"--token",
				"token",
				"--copy",
				"--tapecopies",
				"6571579",
				"--autoarchive",
				"--linkfiles",
				"somerandomstring",
				"--allowexistingsource",
				"--addattachment",
				"random attachment string",
				"--addcaption",
				"a seemingly random caption",
				"--version",
				"argument placeholder",
			},
		},
		// datasetPublishData
		{
			name: "datasetPublishData test without flags",
			flags: map[string]interface{}{
				"publish":       false,
				"testenv":       false,
				"devenv":        false,
				"version":       false,
				"publisheddata": "",
				"user":          "",
				"token":         "",
			},
			args: []string{"datasetPublishData"},
		},
		{
			name: "datasetPublishData test with (almost) all flags set",
			flags: map[string]interface{}{
				"publish":       true,
				"testenv":       true,
				"devenv":        false,
				"version":       true,
				"publisheddata": "some data that was published",
				"user":          "usertest:passtest",
				"token":         "token",
			},
			args: []string{
				"datasetPublishData",
				"--publish",
				"--publisheddata",
				"some data that was published",
				"--testenv",
				"--user",
				"usertest:passtest",
				"--token",
				"token",
				"--version",
			},
		},
		// datasetPublishDataRetrieve
		{
			name: "datasetPublishDataRetrieve test without flags",
			flags: map[string]interface{}{
				"retrieve":      false,
				"testenv":       false,
				"devenv":        false,
				"version":       false,
				"user":          "",
				"token":         "",
				"publisheddata": "",
			},
			args: []string{"datasetPublishDataRetrieve"},
		},
		{
			name: "datasetPublishDataRetrieve test with (almost) all flags set",
			flags: map[string]interface{}{
				"retrieve":      true,
				"testenv":       true,
				"devenv":        false,
				"version":       true,
				"user":          "usertest:passtest",
				"token":         "token",
				"publisheddata": "some data that was published",
			},
			args: []string{
				"datasetPublishDataRetrieve",
				"--retrieve",
				"--testenv",
				"--user",
				"usertest:passtest",
				"--token",
				"token",
				"--publisheddata",
				"some data that was published",
				"--version",
			},
		},
		// datasetRetriever
		{
			name: "datasetRetriever test without flags",
			flags: map[string]interface{}{
				"retrieve":   false,
				"nochksum":   false,
				"testenv":    false,
				"devenv":     false,
				"version":    false,
				"user":       "",
				"token":      "",
				"dataset":    "",
				"ownergroup": "",
			},
			args: []string{"datasetRetriever", "placeholder arg"},
		},
		{
			name: "datasetRetriever test with (almost) all flags set",
			flags: map[string]interface{}{
				"retrieve":   true,
				"nochksum":   true,
				"testenv":    true,
				"devenv":     false,
				"version":    true,
				"user":       "usertest:passtest",
				"token":      "token",
				"dataset":    "some dataset",
				"ownergroup": "some owners",
			},
			args: []string{
				"datasetRetriever",
				"--retrieve",
				"--nochksum",
				"--testenv",
				"--user",
				"usertest:passtest",
				"--token",
				"token",
				"--dataset",
				"some dataset",
				"--ownergroup",
				"some owners",
				"--version",
				"placeholder arg",
			},
		},
		// waitForJobFinished
		{
			name: "waitForJobFinished test without flags",
			flags: map[string]interface{}{
				"testenv": false,
				"devenv":  false,
				"version": false,
				"user":    "",
				"token":   "",
				"job":     "",
			},
			args: []string{"waitForJobFinished"},
		},
		{
			name: "waitForJobFinsihed with (almost) all flags set",
			flags: map[string]interface{}{
				"testenv": true,
				"devenv":  false,
				"version": true,
				"user":    "usertest:passtest",
				"token":   "token",
				"job":     "some job to wait for",
			},
			args: []string{
				"waitForJobFinished",
				"--testenv",
				"--user",
				"usertest:passtest",
				"--token",
				"token",
				"--job",
				"some job to wait for",
				"--version",
			},
		},
	}

	// running test cases
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			//flag.CommandLine = flag.NewFlagSet(test.name, flag.ExitOnError)
			datasetUtils.TestFlags = func(flags map[string]interface{}) {
				passing := true
				for flag := range test.flags {
					if flags[flag] != test.flags[flag] {
						t.Logf("%s's value should be \"%v\" but it's \"%v\", or non-matching type", flag, test.flags[flag], flags[flag])
						passing = false
					}
				}
				if !passing {
					t.Fail()
				}
			}

			rootCmd.PersistentFlags().VisitAll(func(flag *pflag.Flag) {
				flag.Value.Set(flag.DefValue)
				flag.Changed = false
			})

			rootCmd.SetArgs(test.args)
			Execute()
		})
	}
}
