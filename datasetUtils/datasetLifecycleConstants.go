package datasetUtils

type ArchiveStatusMessage string

const (
	ArchiveStatusMessageDatasetCreated              ArchiveStatusMessage = "datasetCreated"
	ArchiveStatusMessageOrigDatablocksNotYetCreated ArchiveStatusMessage = "origDatablocksNotYetCreated"
	ArchiveStatusMessageFilesNotYetAvailable        ArchiveStatusMessage = "filesNotYetAvailable"
)
