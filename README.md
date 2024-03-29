# SciCat CLI tools

## Building

For testing, just build `main.go` for each command:

```
go build -o cmd/datasetIngestor/datasetIngestor cmd/datasetIngestor/main.go
```

To build all applications and target architectures, use the build script:

```
cmd/build.sh
```

Tools are compiled for the following architectures:

- linux / amd64
- windows / amd64
- macos / universal

These can be cross-compiled from any system.

## Deployment

Tools are deployed by committing the binaries to this repository.

1. Run `deploy.sh`. This script

   1. Builds executables for all targets
   2. Commits all changes to the git *rollout* repository.

   After this step the version numbers are out of sync until the deploy steps below are performed.

2. Deploy to the scicat tools repo. This is the public deployment
   - Following this step the commands can be fetched via curl from
     https://gitlab.psi.ch/scicat/tools/$OS

3. Deploy linux versions to online beamline consoles (you need to have write access rights):

```
cd linux
scp datasetArchiver datasetIngestor datasetRetriever  datasetGetProposal datasetCleaner SciCat egli@gfa-lc.psi.ch:/work/sls/bin/
```

4. Deploy linux versions to the ingest server pbaingest01

```
ssh egli@pbaingest01.psi.ch
cd bin/
curl -O  https://gitlab.psi.ch/scicat/tools/raw/master/linux/datasetIngestor;chmod +x datasetIngestor
curl -O  https://gitlab.psi.ch/scicat/tools/raw/master/linux/datasetArchiver;chmod +x datasetArchiver 
curl -O  https://gitlab.psi.ch/scicat/tools/raw/master/linux/datasetGetProposal;chmod +x datasetGetProposal
```

5. Deploy to Ra cluster as a pmodule

```
cd pmodules/buildblocks/Tools/datacatalog
kinit
aklog
# optionally update files/variants.Linux to new version
# the following may be needed first:
#export PMODULES_TMPDIR=/var/tmp/egli
module load Pmodules/1.0.0rc13
./build 1.1.10 -f
```
