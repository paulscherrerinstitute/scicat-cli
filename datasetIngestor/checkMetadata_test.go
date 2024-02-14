package datasetIngestor

import (
    "testing"
)

func TestGetHost(t *testing.T) {
    host := getHost()

    if len(host) == 0 {
        t.Errorf("getHost() returned an empty string")
    }

    if host == "unknown" {
        t.Errorf("getHost() was unable to get the hostname")
    }
}