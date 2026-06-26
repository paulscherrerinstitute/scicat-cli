package cmd

import (
	"github.com/paulscherrerinstitute/scicat-cli/v3/cmd/cliutils"
	"github.com/paulscherrerinstitute/scicat-cli/v3/orchestrator"
	"github.com/spf13/cobra"
)

var datasetIngestorCmd = &cobra.Command{
	Use:   "datasetIngestor",
	Short: "Define and add a dataset to the SciCat datacatalog",
	Long: `Purpose: define and add a dataset to the SciCat datacatalog

This command must be run on the machine having access to the data
which comprises the dataset. It takes one or more arguments and creates
the necessary messages which trigger the creation of the corresponding
datacatalog entries.

Argument formats accepted:

  Single dataset (legacy positional form):
    metadata.json
    metadata.json filelist.txt
    metadata.json folderlisting.txt

  Multiple datasets (@ separator form):
    metadata1.json@filelist1.txt metadata2.json@filelist2.txt ...
    metadata1.json metadata2.json ...

  In the @ form the right-hand side (filelist.txt) is optional.
  Omit the @ entirely to pass only the metadata file.

For further help see "` + cliutils.MANUAL + `"

Special hints for the decentral use case, where data is copied first to intermediate storage:
For Linux you need to have a valid Kerberos tickets, which you can get via the kinit command.
For Windows you need instead to specify -user username:password on the command line.`,
	Args: minArgsWithVersionException(1),
	Run: func(cmd *cobra.Command, args []string) {
		orchestrator.RunIngestionPipeline(cmd, args, VERSION)
	},
}

func init() {
	rootCmd.AddCommand(datasetIngestorCmd)

	datasetIngestorCmd.Flags().Bool("ingest", false, "Defines if this command is meant to actually ingest data")
	datasetIngestorCmd.Flags().Bool("testenv", false, "Use test environment (qa) instead of production environment")
	datasetIngestorCmd.Flags().Bool("devenv", false, "Use development environment instead of production environment (developers only)")
	datasetIngestorCmd.Flags().Bool("localenv", false, "Use local environment instead of production environment (developers only)")
	datasetIngestorCmd.Flags().Bool("tunnelenv", false, "Use tunneled API server at port 5443 to access development instance (developers only)")
	datasetIngestorCmd.Flags().String("rsync-url", "", "Custom URL for the rsync server. It is a complementary parameter 'scicat-url', but is not required. When not given, the chosen environment's RSYNC server is used.")
	datasetIngestorCmd.Flags().Bool("noninteractive", false, "If set no questions will be asked and the default settings for all undefined flags will be assumed")
	datasetIngestorCmd.Flags().Bool("copy", false, "Defines if files should be copied from your local system to a central server before ingest (i.e. your data is not centrally available and therefore needs to be copied ='decentral' case). copyFlag has higher priority than nocopyFlag. If neither flag is defined the tool will try to make the best guess.")
	datasetIngestorCmd.Flags().Bool("nocopy", false, "Defines if files should *not* be copied from your local system to a central server before ingest (i.e. your data is centrally available and therefore does not need to be copied ='central' case).")
	datasetIngestorCmd.Flags().String("transfer-type", "ssh", "Selects the transfer type to be used for transferring files. Available options: \"ssh\", \"globus\"")
	datasetIngestorCmd.Flags().Int("tapecopies", 0, "Number of tapecopies to be used for archiving")
	datasetIngestorCmd.Flags().Bool("autoarchive", false, "Option to create archive job automatically after ingestion")
	datasetIngestorCmd.Flags().String("linkfiles", "keepInternalOnly", "Define what to do with symbolic links: (keep|delete|keepInternalOnly)")
	datasetIngestorCmd.Flags().Bool("allowexistingsource", false, "Defines if existing sourceFolders can be reused")
	datasetIngestorCmd.Flags().String("addattachment", "", "Filename of image to attach (single dataset case only)")
	datasetIngestorCmd.Flags().String("addcaption", "", "Optional caption to be stored with attachment (single dataset case only)")
	datasetIngestorCmd.Flags().String("globus-cfg", "", "Override globus transfer config file location [default: globus.yaml next to executable]")

	datasetIngestorCmd.MarkFlagsMutuallyExclusive("testenv", "devenv", "localenv", "tunnelenv")
	datasetIngestorCmd.MarkFlagsMutuallyExclusive("nocopy", "copy")
}
