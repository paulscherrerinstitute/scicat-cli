# SciCat CLI tools

## Building

For testing, just build `main.go` for each command:

```
go build -o cmd/datasetIngestor/datasetIngestor cmd/datasetIngestor/main.go
```

All applications are built automatically and can be downloaded from the [Releases](https://github.com/paulscherrerinstitute/scicat-cli/releases) section of this repo.

To build the applications and target architectures locally, use GoReleaser. Check `.goreleaser.yaml` to see the configurations.
To use GoReleaser, you can run the command `goreleaser release --snapshot --clean` in your terminal. This will build the binaries, create the archives and generate the changelog. The `--snapshot flag` ensures that no publishing will happen.
Before running this command, you should ensure that you have [installed GoReleaser](https://goreleaser.com/install/).

Tools are compiled for the following architectures:

- linux / amd64
- windows / amd64
- macos / universal

These can be cross-compiled from any system.

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

*(Outdated)*

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
