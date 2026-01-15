# Migrating to scicat-cli version 3

Version 3 introduces a major change in the scicat CLI. Rather than many executables
(datasetIngestor, datasetRetriever, etc), the scicat CLI is a single binary called
`scicat` which uses subcommands for different actions. This makes it easier to use and
more consistent with other tools.

This page summarizes changes to allow users familiar with the old tools to transition to
the new version.

## Subcommands

| Old command                | New command     |
| -------------------------- | --------------- |
| datasetIngestor            | scicat-cli ingest   |
| datasetRetriever           | scicat-cli retrieve |
| datasetArchiver            | scicat-cli archive  |
| datasetCleaner             | scicat-cli clean    |
| datasetGetProposal         | scicat-cli proposal |
| datasetPublishData         | _deprecated_    |
| datasetPublishDataRetrieve | _deprecated_    |
| waitForJobFinished         | scicat-cli wait     |

Old commands are also valid aliases for subcommands (eg `scicat-cli datasetIngestor`).

## Command line options

Previously all options were treated as named arguments and accepted with either one or
two hyphens. Options now follow standard conventions, with two dashes for long named
arguments and a single dash for single-letter abbreviations. Thus `-token` will no longer be a
synonym for `--token` but would be interpreted as `-t -o -k -e -n`.

## Examples

### Syntax

Both the old command using the `flags` parser and new command using the `cobra` parser are shown. Diff syntax is used for highlighting. For example:

```diff
- old command (<3.0.0)
+ new cobra command (>=3.0.0)
```

### Ingestion

#### Ingestion dry run

```diff
- datasetIngestor metadata.json
+ scicat-cli ingest metadata.json
```

#### Ingestion with file listing

`filelisting.txt` contains one path per line, where paths are relative to the `sourceFolder` entry in metadata.json

```diff
- datasetIngestor metadata.json filelisting.txt
+ scicat-cli ingest --filelist filelisting.txt metadata.json
```

#### Multi-dataset ingestion using folder listing

Previous versions supported a 'folderlisting.txt' argument, which was used to ingest
multiple datasets with one command. This feature has been removed, but can be
implemented with a short shell loop.

```diff
- datasetIngestor metadata.json folderlisting.txt
+ while read folder; do scicat-cli ingest --no-interactive -DsourceFolder="$folder" metadata.json; done <folderlisting.txt
```

This takes advantage of the `-D` flag to override the sourceFolder for each dataset.

### Retrieve

#### List available datasets on the retrieval cache

This lists datasets which have been successfully retrieved from tape (eg by selecting
'Retrieve to PSI' from SciCat) and are available on the PSI cache.

```diff
- datasetRetriever -token $SCICAT_TOKEN destination/
+ scicat-cli retrieve --token $SCICAT_TOKEN
```

#### Retrieve a single dataset

Retrieve dataset `PID` to a folder, keeping the original `sourceLocation` as folders in
the destination.

```diff
- datasetRetriever -token $SCICAT_TOKEN -retrieve -dataset PID destination/
+ scicat-cli retrieve --token $SCICAT_TOKEN -C destination/ --no-strip-path PID
```

#### Retrieve all available datasets

```diff
- datasetRetriever -token $SCICAT_TOKEN -retrieve destination/
+ scicat-cli retrieve --token $SCICAT_TOKEN -C destination/ --no-strip-path --all
```

#### Retrieve datasets with a given ownerGroup

```diff
- datasetRetriever -retrieve -ownergroup unx-group destination/
+ scicat-cli retrieve -C destination/ --no-strip-path --filter ownerGroup==unx-group
```
