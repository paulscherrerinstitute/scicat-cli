// +build aix darwin dragonfly freebsd js,wasm linux nacl netbsd openbsd solaris

package datasetIngestor

import (
	"testing"
)

func TestGetRsyncVersion(t *testing.T) {
	version, err := getRsyncVersion()
	if err != nil {
		t.Errorf("getRsyncVersion() returned an error: %v", err)
	}
	if version == "" {
		t.Error("getRsyncVersion() returned an empty string")
	}
}
