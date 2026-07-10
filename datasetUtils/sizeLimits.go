package datasetUtils

// IngestSizeLimits bounds the size of a single dataset ingest: how many files
// it may contain in total, and how files are grouped into orig datablocks.
type IngestSizeLimits struct {
	TotalMaxFiles int64 // max number of files allowed in one dataset
	BlockMaxFiles int   // max number of files per orig datablock
	BlockMaxBytes int64 // max total bytes per orig datablock
}

var DefaultIngestSizeLimits = IngestSizeLimits{
	TotalMaxFiles: 1000000,
	BlockMaxFiles: 20000,        // 20 for testing the logic
	BlockMaxBytes: 200000000000, // 700000 for testing the logic
}
