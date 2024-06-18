package main

import (
	"flag"
	"os"
	"testing"

	"github.com/paulscherrerinstitute/scicat/datasetUtils"
)

func TestMainFlags(t *testing.T) {
	// test cases
	tests := []struct {
		name  string
		flags map[string]interface{}
		args  []string
	}{
		{
			name: "Test without flags",
			flags: map[string]interface{}{
				"testenv":           false,
				"devenv":            false,
				"nonInteractive":    false,
				"removeFromCatalog": false,
				"version":           false,
				"user":              "",
				"token":             "",
			},
			args: []string{"test"},
		},
		{
			name: "Set all flags",
			flags: map[string]interface{}{
				"testenv":           true,
				"devenv":            true,
				"nonInteractive":    true,
				"removeFromCatalog": true,
				"version":           true,
				"user":              "usertest:passtest",
				"token":             "token",
			},
			args: []string{
				"test",
				"--testenv",
				"--devenv",
				"--nonInteractive",
				"--removeFromCatalog",
				"--user",
				"usertest:passtest",
				"--token",
				"token",
				"--version",
			},
		},
	}

	// running test cases
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			flag.CommandLine = flag.NewFlagSet(test.name, flag.ExitOnError)
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

			os.Args = test.args
			main()
		})
	}
}
