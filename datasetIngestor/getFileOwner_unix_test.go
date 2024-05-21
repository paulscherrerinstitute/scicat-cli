// +build aix darwin dragonfly freebsd js,wasm linux nacl netbsd openbsd solaris

package datasetIngestor

import (
	"os"
	"os/user"
	"testing"
	"io/ioutil"
)

func TestGetFileOwner(t *testing.T) {
	// Create a temporary file
	tmpfile, err := ioutil.TempFile("", "example")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpfile.Name()) // clean up
	
	// Get the current user
	currentUser, err := user.Current()
	if err != nil {
		t.Fatal(err)
	}
	
	// Get the file info
	fileInfo, err := tmpfile.Stat()
	if err != nil {
		t.Fatal(err)
	}
	
	// Get the file owner
	uidName, _ := GetFileOwner(fileInfo)
	
	// Check if the file owner matches the current user
	if uidName != currentUser.Username {
		t.Errorf("Expected %s, got %s", currentUser.Username, uidName)
	}

	// Check owner of a non-existent file
	_, err = os.Stat("non_existent_file")
	if err == nil {
			t.Fatal("Expected error, got none")
	}
}
