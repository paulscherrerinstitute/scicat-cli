package main

import (
	"bytes"
	"os"
	"testing"
)

// TestMainHelp is a test function that verifies the output of the main function.
// It captures the stdout, runs the main function, and checks if the output contains the expected strings.
// This just checks if the main function prints the help message.
func TestMainHelp(t *testing.T) {
	// Capture stdout
	// The variable 'old' stores the original value of the standard output (os.Stdout).
	old := os.Stdout
	// r is a ReadCloser that represents the read end of the pipe.
	// w is a WriteCloser that represents the write end of the pipe.
	// err is an error variable.
	// The os.Pipe() function in Go is used to create a synchronous in-memory pipe. It can be used for communication between different parts of the program.
	// The `os.Pipe()` function in Go is used to create a synchronous in-memory pipe. It can be used for communication between different parts of your program.
	// This function returns two values: a `*os.File` for reading and a `*os.File` for writing. When you write data to the write end of the pipe, it becomes available to read from the read end of the pipe. This can be useful for passing data between goroutines or between different parts of your program without using the disk.
	r, w, err1 := os.Pipe()
	if err1 != nil {
		// The Fatalf method is similar to log.Fatalf or fmt.Printf in that it formats a string according to a format specifier and arguments, then logs that string as an error message. However, in addition to this, Fatalf also ends the test immediately. No further code in the test function will be executed, and the test will be marked as failed.
    t.Fatalf("Could not start the test. Error in reading the file: %v", err1)
	}
	// redirect the standard output (os.Stdout) to a different destination, represented by w.
	// By default, anything written to os.Stdout will be printed to the terminal.
	// The w in this line of code is expected to be a value that satisfies the io.Writer interface, which means it has a Write method. This could be a file, a buffer, a network connection, or any other type of destination for output.
	// Since w is connected to r, anything written to w can be read from r. This is how we will capture the output of the main function.
	os.Stdout = w

	// Restore stdout after the test
	// defer is a keyword that schedules a function call to be run after the function that contains the defer statement has completed.
	defer func() {
		os.Stdout = old
	}()

	// Run main function (assuming your main function does not take any arguments)
	main()

	// Close pipe writer to flush the output
	w.Close()

	//declares a variable named buf of type bytes.Buffer. The bytes.Buffer type is a struct provided by the Go standard library that implements the io.Reader and io.Writer interfaces.
	var buf bytes.Buffer
	// Copy pipe reader output to buf
	// ReadFrom reads data from the given reader r and writes it to the buffer buf.
	// It returns the number of bytes read and any error encountered.
	_, err := buf.ReadFrom(r)
	if err != nil {
		t.Fatalf("Error reading output: %v", err)
	}

	// Check if the output contains expected strings
	expected := "\n\nTool to ingest datasets to the data catalog.\n\n"
	//The bytes.Contains function takes two arguments, both of which are slices of bytes, and checks if the second argument is contained within the first.
	// Here, expected is a string, and []byte(expected) converts that string to a slice of bytes.
	if !bytes.Contains(buf.Bytes(), []byte(expected)) {
		t.Errorf("Expected output %q not found in %q", expected, buf.String())
	}
}
