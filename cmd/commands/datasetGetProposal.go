package cmd

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/fatih/color"
	"github.com/paulscherrerinstitute/scicat/datasetUtils"
	"github.com/spf13/cobra"
)

var datasetGetProposalCmd = &cobra.Command{
	Use:   "datasetGetProposal [options] ownerGroup",
	Short: "Returns the proposal information for a given ownerGroup",
	Long: `Tool to retrieve proposal information for a given ownerGroup.
	
For further help see "` + MANUAL + `"`,
	Args: exactArgsWithVersionException(1),
	Run: func(cmd *cobra.Command, args []string) {
		// vars and constants
		var client = &http.Client{
			Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: false}},
			Timeout:   10 * time.Second}

		const APP = "datasetGetProposal"

		var APIServer string = PROD_API_SERVER
		var env string = "production"

		// pass parameters
		userpass, _ := cmd.Flags().GetString("user")
		token, _ := cmd.Flags().GetString("token")
		fieldname, _ := cmd.Flags().GetString("field")
		testenvFlag, _ := cmd.Flags().GetBool("testenv")
		devenvFlag, _ := cmd.Flags().GetBool("devenv")
		showVersion, _ := cmd.Flags().GetBool("version")

		if datasetUtils.TestFlags != nil {
			datasetUtils.TestFlags(map[string]interface{}{
				"user":    userpass,
				"token":   token,
				"field":   fieldname,
				"testenv": testenvFlag,
				"devenv":  devenvFlag,
				"version": showVersion,
			})
			return
		}

		// execute command
		if showVersion {
			fmt.Printf("%s\n", VERSION)
			return
		}

		// check for program version only if running interactively
		datasetUtils.CheckForNewVersion(client, APP, VERSION)

		if devenvFlag {
			APIServer = DEV_API_SERVER
			env = "dev"
		}
		if testenvFlag {
			APIServer = TEST_API_SERVER
			env = "test"
		}

		color.Set(color.FgGreen)
		log.Printf("You are about to retrieve the proposal information from the === %s === data catalog environment...", env)
		color.Unset()

		//TODO cleanup text formatting:
		if len(args) != 1 {
			log.Fatalln("invalid number of args")
		}
		ownerGroup := args[0]

		user, accessGroups := authenticate(RealAuthenticator{}, client, APIServer, userpass, token)
		proposal, err := datasetUtils.GetProposal(client, APIServer, ownerGroup, user, accessGroups)
		if err != nil {
			log.Fatal(err)
		}

		// proposal is of type map[string]interface{}

		if len(proposal) <= 0 {
			log.Fatalf("No Proposal information found for group %v\n", ownerGroup)
		}

		if fieldname != "" {
			fmt.Println(proposal[fieldname])
		} else {
			pretty, _ := json.MarshalIndent(proposal, "", "    ")
			fmt.Printf("%s\n", pretty)
		}
	},
}

func init() {
	rootCmd.AddCommand(datasetGetProposalCmd)

	datasetGetProposalCmd.Flags().String("field", "", "Defines optional field name , whose value should be returned instead of full information")
	datasetGetProposalCmd.Flags().Bool("testenv", false, "Use test environment (qa) instead or production")
	datasetGetProposalCmd.Flags().Bool("devenv", false, "Use development environment instead or production")

	datasetGetProposalCmd.MarkFlagsMutuallyExclusive("testenv", "devenv")
}
