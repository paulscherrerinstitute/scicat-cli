package datasetIngestor

import (
	"os"
)

func GetFileOwner(f os.FileInfo) (uidName string, gidName string) {

	uidName = ""
	gidName = ""
	return uidName, gidName
}
