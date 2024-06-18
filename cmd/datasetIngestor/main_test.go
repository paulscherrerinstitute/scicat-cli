package main

import (
	"bytes"
	"flag"
	"os"
	"testing"

	"github.com/paulscherrerinstitute/scicat/datasetUtils"
)

// TestMainOutput is a test function that verifies the output of the main function.
// It captures the stdout, runs the main function, and checks if the output contains the expected strings.
// This just checks if the main function prints the help message.
func TestMainOutput(t *testing.T) {
	flag.CommandLine = flag.NewFlagSet("flag output", flag.ContinueOnError)
	os.Args = []string{"test"}

	os.Setenv("TEST_MODE", "true")
	oldTestMode := "false"
	defer os.Setenv("TEST_MODE", oldTestMode)
	// Capture stdout
	// The variable 'oldO' stores the original value of the standard output (os.Stdout).
	oldO := os.Stdout
	oldE := os.Stderr
	// rO is a ReadCloser that represents the read end of the pipe.
	// wO is a WriteCloser that represents the write end of the pipe.
	// err is an error variable.
	// The os.Pipe() function in Go is used to create a synchronous in-memory pipe. It can be used for communication between different parts of the program.
	// The `os.Pipe()` function in Go is used to create a synchronous in-memory pipe. It can be used for communication between different parts of your program.
	// This function returns two values: a `*os.File` for reading and a `*os.File` for writing. When you write data to the write end of the pipe, it becomes available to read from the read end of the pipe. This can be useful for passing data between goroutines or between different parts of your program without using the disk.
	rO, wO, errO := os.Pipe()
	_, wE, errE := os.Pipe()
	if errO != nil || errE != nil {
		// The Fatalf method is similar to log.Fatalf or fmt.Printf in that it formats a string according to a format specifier and arguments, then logs that string as an error message. However, in addition to this, Fatalf also ends the test immediately. No further code in the test function will be executed, and the test will be marked as failed.
		t.Fatalf("Could not start the test. Error in reading the file: %v", errO)
	}
	// redirect the standard output (os.Stdout) to a different destination, represented by w.
	// also redirect stderr to hide it only
	// By default, anything written to os.Stdout will be printed to the terminal.
	// The w in this line of code is expected to be a value that satisfies the io.Writer interface, which means it has a Write method. This could be a file, a buffer, a network connection, or any other type of destination for output.
	// Since w is connected to r, anything written to w can be read from r. This is how we will capture the output of the main function.
	os.Stdout = wO
	os.Stderr = wE

	// Run main function (assuming your main function does not take any arguments)
	main()

	// Restore stdout & stderr after running main
	os.Stdout = oldO
	os.Stderr = oldE

	// Close pipe writer to flush the output
	wO.Close()
	wE.Close()

	//declares a variable named buf of type bytes.Buffer. The bytes.Buffer type is a struct provided by the Go standard library that implements the io.Reader and io.Writer interfaces.
	var buf bytes.Buffer
	// Copy pipe reader output to buf
	// ReadFrom reads data from the given reader r and writes it to the buffer buf.
	// It returns the number of bytes read and any error encountered.
	_, err := buf.ReadFrom(rO)
	if err != nil {
		t.Fatalf("Error reading output: %v", err)
	}

	// Check if the output contains expected strings
	expected := "\n\nTool to ingest datasets to the data catalog.\n\n"
	if !bytes.Contains(buf.Bytes(), []byte(expected)) {
		t.Errorf("Expected output %q not found in %q", expected, buf.String())
	}
}

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
			args: []string{"test"},
		},
		{
			name: "Set all flags",
			flags: map[string]interface{}{
				"ingest":              true,
				"testenv":             true,
				"devenv":              true,
				"localenv":            true,
				"tunnelenv":           true,
				"noninteractive":      true,
				"copy":                true,
				"nocopy":              true,
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
				"test",
				"--ingest",
				"--testenv",
				"--devenv",
				"--localenv",
				"--tunnelenv",
				"--noninteractive",
				"--user",
				"usertest:passtest",
				"--token",
				"token",
				"--copy",
				"--nocopy",
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
