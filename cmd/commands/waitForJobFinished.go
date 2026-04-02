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
	"github.com/paulscherrerinstitute/scicat-cli/v3/datasetUtils"
	"github.com/spf13/cobra"
)

var waitForJobFinishedCmd = &cobra.Command{
	Aliases: []string{"w", "wait"},
	Use:     "waitForJobFinished (options)",
	Short:   "Waits for job to be finished",
	Long:    `This script polls the status of a given job and returns when Job is finished`,
	Args:    cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		// consts & vars
		var client = &http.Client{
			Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: false}},
			Timeout:   10 * time.Second}

		var APIServer string = PROD_API_SERVER
		var env string = "production"

		// structs
		type Job struct {
			Id            string `json:"id"`
			StatusMessage string `json:"statusMessage"`
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

			var jobDetails Job
			err = json.Unmarshal(body, &jobDetails)
			if err != nil {
				return true, err
			}
			if jobDetails.StatusMessage == "finished" {
				return true, nil
			} else {
				return false, nil
			}
		}

		// retrieve flags
		userpass, _ := cmd.Flags().GetString("user")
		token, _ := cmd.Flags().GetString("token")
		oidc, _ := cmd.Flags().GetBool("oidc")
		jobId, _ := cmd.Flags().GetString("job") // shouldn't jobID be a positional argument? it's obligatory
		testenvFlag, _ := cmd.Flags().GetBool("testenv")
		devenvFlag, _ := cmd.Flags().GetBool("devenv")
		scicatUrl, _ := cmd.Flags().GetString("scicat-url")
		localenvFlag, _ := cmd.Flags().GetBool("localenv")
		showVersion, _ := cmd.Flags().GetBool("version")

		if datasetUtils.TestFlags != nil {
			datasetUtils.TestFlags(map[string]interface{}{
				"user":       userpass,
				"token":      token,
				"job":        jobId,
				"testenv":    testenvFlag,
				"devenv":     devenvFlag,
				"scicat-url": scicatUrl,
				"version":    showVersion,
			})
			return
		}

		// command
		if showVersion {
			fmt.Printf("%s\n", VERSION)
			return
		}

		if localenvFlag {
			APIServer = LOCAL_API_SERVER
			env = "local"
		}
		if devenvFlag {
			APIServer = DEV_API_SERVER
			env = "dev"
		}
		if testenvFlag {
			APIServer = TEST_API_SERVER
			env = "test"
		}
		if scicatUrl != "" {
			APIServer = scicatUrl
			env = "custom"
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

		user, _, err := authenticate(RealAuthenticator{}, client, APIServer, userpass, token, oidc)
		if err != nil {
			log.Fatal(err)
		}

		req, err := http.NewRequest("GET", APIServer+"/Jobs/"+url.QueryEscape(jobId), nil)
		if err != nil {
			log.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer "+user["accessToken"])
		req.Header.Set("accept", "application/json")

		timeoutchan := make(chan bool)
		ticker := time.NewTicker(5 * time.Second)
		quit := make(chan struct{})
		go func() {
			for {
				select {
				case <-ticker.C:
					resp, err := client.Do(req)
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
	waitForJobFinishedCmd.Flags().Bool("localenv", false, "Use local environment (local) instead or production")

	waitForJobFinishedCmd.MarkFlagsMutuallyExclusive("testenv", "devenv", "localenv")
}
