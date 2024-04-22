// +build aix darwin dragonfly freebsd js,wasm linux nacl netbsd openbsd solaris

package datasetIngestor

import (
	"testing"
	"regexp"
)

func TestGetRsyncVersion(t *testing.T) {
	version, err := getRsyncVersion()
	if err != nil {
		t.Errorf("getRsyncVersion() returned an error: %v", err)
	}
	if version == "" {
		t.Error("getRsyncVersion() returned an empty string")
	} else {
		match, _ := regexp.MatchString(`^\d{1,2}\.\d{1,2}\.\d{1,2}$`, version)
		if !match {
			t.Error("getRsyncVersion() returned wrong version string format: ", version)
		}
	}
}
