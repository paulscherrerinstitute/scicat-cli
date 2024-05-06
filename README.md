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

* Deploy linux versions to online beamline consoles (you need to have write access rights):

```bash
cd linux
scp datasetArchiver datasetIngestor datasetRetriever  datasetGetProposal datasetCleaner SciCat egli@gfa-lc.psi.ch:/work/sls/bin/
```

* Deploy linux versions to the ingest server pbaingest01

```bash
ssh egli@pbaingest01.psi.ch
cd bin/
curl -s https://api.github.com/repos/paulscherrerinstitute/scicat-cli/releases/latest \
| grep "browser_download_url.*Linux*tar.gz" \
| cut -d : -f 2,3 \
| tr -d \" \
| wget -qi -
tar -xzf scicat-cli_*_Linux_x86_64.tar.gz
chmod +x datasetIngestor datasetArchiver datasetGetProposal
```

* Deploy to Ra cluster as a pmodule

```bash
cd pmodules/buildblocks/Tools/datacatalog
kinit
aklog
# optionally update files/variants.Linux to new version
# the following may be needed first:
#export PMODULES_TMPDIR=/var/tmp/egli
module load Pmodules/1.0.0rc13
./build 1.1.10 -f
```
