package cmd

import (
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"time"

	"github.com/fatih/color"
	"github.com/paulscherrerinstitute/scicat/datasetUtils"
	"github.com/spf13/cobra"
)

var waitForJobFinishedCmd = &cobra.Command{
	Use:   "waitForJobFinished (options)",
	Short: "Waits for job to be finished",
	Long:  `This script polls the status of a given job and returns when Job is finished`,
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		// consts & vars
		var client = &http.Client{
			Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: false}},
			Timeout:   10 * time.Second}

		const PROD_API_SERVER string = "https://dacat.psi.ch/api/v3"
		const TEST_API_SERVER string = "https://dacat-qa.psi.ch/api/v3"
		const DEV_API_SERVER string = "https://dacat-development.psi.ch/api/v3"

		var APIServer string = PROD_API_SERVER
		var env string = "production"

		// structs
		type Job struct {
			Id               string
			JobStatusMessage string
		}

		// funcs
		handlePollResponse := func(resp *http.Response) (stopPolling bool, err error) {
			if resp.StatusCode != 200 {
				return true, fmt.Errorf("querying dataset details failed with status code %v", resp.StatusCode)
			}
			defer resp.Body.Close()

			body, err := io.ReadAll(resp.Body)
			if err != nil {
				return true, err
			}

			var jobDetails []Job
			err = json.Unmarshal(body, &jobDetails)
			if err != nil {
				return true, err
			}
			if len(jobDetails) == 0 {
				return false, nil
			}
			if jobDetails[0].JobStatusMessage == "finished" {
				return true, nil
			} else {
				return false, nil
			}
		}

		// retrieve flags
		userpass, _ := cmd.Flags().GetString("user")
		token, _ := cmd.Flags().GetString("token")
		jobId, _ := cmd.Flags().GetString("job") // shouldn't jobID be a positional argument? it's obligatory
		testenvFlag, _ := cmd.Flags().GetBool("testenv")
		devenvFlag, _ := cmd.Flags().GetBool("devenv")
		showVersion, _ := cmd.Flags().GetBool("version")

		if datasetUtils.TestFlags != nil {
			datasetUtils.TestFlags(map[string]interface{}{
				"user":    userpass,
				"token":   token,
				"job":     jobId,
				"testenv": testenvFlag,
				"devenv":  devenvFlag,
				"version": showVersion,
			})
			return
		}

		// command
		if showVersion {
			fmt.Printf("%s\n", VERSION)
			return
		}

		if devenvFlag {
			APIServer = DEV_API_SERVER
			env = "dev"
		}
		if testenvFlag {
			APIServer = TEST_API_SERVER
			env = "test"
		}

		color.Set(color.FgGreen)
		log.Printf("You are about to wait for a job to be finished from the === %s === API server...", env)
		color.Unset()

		if jobId == "" { /* && *datasetId == "" && *ownerGroup == "" */
			fmt.Println("\n\nTool to wait for job to be finished")
			fmt.Printf("Run script without arguments, but specify options:\n\n")
			fmt.Printf("waitForJobFinished [options] \n\n")
			fmt.Printf("Use -job option to define the job that should be polled.\n\n")
			fmt.Printf("For example:\n")
			fmt.Printf("./waitForJobFinished -job ... \n\n")
			flag.PrintDefaults()
			return
		}

		user, _ := authenticate(RealAuthenticator{}, client, APIServer, userpass, token)

		filter := `{"where":{"id":"` + jobId + `"}}`

		v := url.Values{}
		v.Set("filter", filter)
		v.Add("access_token", user["accessToken"])

		var myurl = APIServer + "/Jobs?" + v.Encode()

		timeoutchan := make(chan bool)
		ticker := time.NewTicker(5 * time.Second)
		quit := make(chan struct{})
		go func() {
			for {
				select {
				case <-ticker.C:
					resp, err := client.Get(myurl)
					if err != nil {
						log.Fatal("Get Job failed:", err)
					}
					stopPolling, err := handlePollResponse(resp)
					if stopPolling {
						if err != nil {
							fmt.Println(err)
						} else {
							fmt.Println("finished")
						}
						ticker.Stop()
						timeoutchan <- true
					}
				case <-quit:
					ticker.Stop()
					timeoutchan <- true
				}
			}
		}()

		select {
		case <-timeoutchan:
			break
		case <-time.After(time.Hour * 24):
			break
		}
	},
}

func init() {
	rootCmd.AddCommand(waitForJobFinishedCmd)

	waitForJobFinishedCmd.Flags().String("job", "", "Defines the job id to poll")
	waitForJobFinishedCmd.Flags().Bool("testenv", false, "Use test environment (qa) instead or production")
	waitForJobFinishedCmd.Flags().Bool("devenv", false, "Use development environment instead or production")

	waitForJobFinishedCmd.MarkFlagsMutuallyExclusive("testenv", "devenv")
}
