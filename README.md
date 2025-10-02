# SciCat CLI tools

## Building

### General Information

For testing, build the CLI tool as follows:

```sh
cd cmd
go build -o scicat-cli
```

The CLI is built automatically by CI upon tagging and can be downloaded from the [Releases](https://github.com/paulscherrerinstitute/scicat-cli/releases) section of this repo.

To build the applications and target architectures locally, use GoReleaser. Check `.goreleaser.yaml` to see the configurations.
To use GoReleaser, you can run the command `goreleaser release --snapshot --clean` in your terminal. This will build the binaries, create the archives and generate the changelog. The `--snapshot flag` ensures that no publishing will happen.
Before running this command, you should ensure that you have [installed GoReleaser](https://goreleaser.com/install/).

Tools are compiled for the following architectures:

- linux / amd64
- windows / amd64
- macos / universal

These can be cross-compiled from any system.

### Testing

Run all tests:

```sh
go test ./...
```

Lint requires golangci-lint version v2.1.0:

```sh
golangci-lint run
```

## V3 Changes

The separate executables (like `datasetIngestor`, `datasetRetriever`...) were combined into one `scicat-cli` executable, with each executable's features available as commands given as the first parameter to this executable.

These commands bear the same names as the former executables. The general syntax change is that if you called `./[COMMAND] [flags]` before, now it's `./scicat-cli [COMMAND] [flags]`.

 Furthermore, the use of single hyphen, multi-letter flags is now discontinued, as it went against general convention. So, in practical terms, `-[long_flag_name]` and `--[long_flag_name]` were both accepted, but now only the latter is accepted.

### Backwards compatibility with v2

A set of shell scripts are included with releases that are compatible with Linux and Mac executables in order to maintain compatibility with preexisting automation scripts.
As these are written in BASH, Windows is not supported unless WSL2 is used.

Usage informations:

- The scripts can be found on the [Releases](https://github.com/paulscherrerinstitute/scicat-cli/releases) page, under scicat-cli_V2Scripts-[VERSION].tar.gz
- Just extract the archive into the folder with the scicat-cli executable
- The scripts and the scicat-cli executable **must** be kept in the **same folder** for them to work
- The scripts will still accept single hyphen flags as well

## Deployment

PSI deploys tools to the following locations.

### Pmodule

This provides the tools to the PSI clusters (ra and merlin) and linux workstations.

Detailed instructions are provided with the [datacatalog
buildblock](https://gitlab.psi.ch/Pmodules/buildblocks/-/tree/master/Tools/datacatalog).

The module downloads the latest release. It can be run from any linux system with AFS
access. Spencer typically uses `pmod7.psi.ch` as his `-adm` user for building modules.

```bash
cd buildblocks/Tools/datacatalog
kinit
aklog
# optionally update files/variants.Linux to new version
# the following may be needed first:
#export PMODULES_TMPDIR=/var/tmp/$USER
module load Pmodules/1.0.0
./build 1.1.10 -f
```

As described in the buildblock README, the
[GUI](https://git.psi.ch/MELANIE/rollout/-/tree/master/Software/00-General/SciCatArchiverGUI)
needs to be compiled separately and manually copied to AFS.

### Manual deployment and upgrade

This can be followed to deploy or upgrade the tool manually:

1. Go to the GitHub [releases page](https://github.com/paulscherrerinstitute/scicat-cli/releases)

2. Choose the release of interest (`latest` is recommended)

3. Download the file from the `Assets` of the chosen release, making sure to select the one compatible with your OS

4. Decompress the asset

5. Open the folder and run the required APP (grant `execute` permissions if required)

#### One liner for Linux systems

```bash
curl -s 'https://api.github.com/repos/paulscherrerinstitute/scicat-cli/releases/latest' \
    | jq -r '.assets[].browser_download_url | select(test("Linux"))' \
    | wget -qi - -O - \
    | tar -xz
```

The latest binaries will be downloaded to `scicat-cli_*/`.

### Beamline consoles

Note: *Outdated instructions*

Deploy linux versions to online beamline consoles (you need to have write access rights):

```bash
cd linux
scp datasetArchiver datasetIngestor datasetRetriever  datasetGetProposal datasetCleaner SciCat egli@gfa-lc.psi.ch:/work/sls/bin/
```

### PBAIngest Server

Deploy linux versions to the ingest server pbaingest01. This is usually done by Michael
Kallmeier-Glanz.

```bash
ssh egli@pbaingest01.psi.ch
cd bin/
curl -s https://api.github.com/repos/paulscherrerinstitute/scicat-cli/releases/latest \
| grep "browser_download_url.*Linux.*tar.gz" \
| cut -d : -f 2,3 \
| tr -d \" \
| wget -qi -
tar -xzf scicat-cli_*_Linux_x86_64.tar.gz
chmod +x datasetIngestor datasetArchiver datasetGetProposal
```
