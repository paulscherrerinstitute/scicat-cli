/*

This script polls the status of a given job and returns when Job is finished

*/

package main

import (
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"github.com/paulscherrerinstitute/scicat/datasetUtils"
	"time"

	"github.com/fatih/color"
)

type Job struct {
	Id               string
	JobStatusMessage string
}

var VERSION string

func main() {
	var client = &http.Client{
		Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: false}},
		Timeout:   10 * time.Second}

	const PROD_API_SERVER string = "https://dacat.psi.ch/api/v3"
	const TEST_API_SERVER string = "https://dacat-qa.psi.ch/api/v3"
	const DEV_API_SERVER string = "https://dacat-development.psi.ch/api/v3"

	const MANUAL string = "http://melanie.gitpages.psi.ch/SciCatPages"
	const APP = "waitForJobFinished"

	var APIServer string
	var env string

	// pass parameters
	userpass := flag.String("user", "", "Defines optional username and password")
	token := flag.String("token", "", "Defines optional API token instead of username:password")
	jobId := flag.String("job", "", "Defines the job id to poll")
	testenvFlag := flag.Bool("testenv", false, "Use test environment (qa) instead or production")
	devenvFlag := flag.Bool("devenv", false, "Use development environment instead or production")
	showVersion := flag.Bool("version", false, "Show version number and exit")

	flag.Parse()

	if *showVersion {
		fmt.Printf("%s\n", VERSION)
		return
	}

	if *testenvFlag {
		APIServer = TEST_API_SERVER
		env = "test"
	} else if *devenvFlag {
		APIServer = DEV_API_SERVER
		env = "dev"
	} else {
		APIServer = PROD_API_SERVER
		env = "production"
	}

	color.Set(color.FgGreen)
	log.Printf("You are about to wait for a job to be finished from the === %s === API server...", env)
	color.Unset()

	if *jobId == "" { /* && *datasetId == "" && *ownerGroup == "" */
		fmt.Println("\n\nTool to wait for job to be finished")
		fmt.Printf("Run script without arguments, but specify options:\n\n")
		fmt.Printf("waitForJobFinished [options] \n\n")
		fmt.Printf("Use -job option to define the job that should be polled.\n\n")
		fmt.Printf("For example:\n")
		fmt.Printf("./waitForJobFinished -job ... \n\n")
		flag.PrintDefaults()
		return
	}

	auth := &datasetUtils.RealAuthenticator{}
	user, _ := datasetUtils.Authenticate(auth, client, APIServer, token, userpass)

	filter := `{"where":{"id":"` + *jobId + `"}}`

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
				defer resp.Body.Close()

				if resp.StatusCode == 200 {
					body, _ := ioutil.ReadAll(resp.Body)
					jobDetails := make([]Job, 0)
					_ = json.Unmarshal(body, &jobDetails)
					if len(jobDetails) > 0 {
						// log.Printf("Job:%v", jobDetails[0].JobStatusMessage)
						message := jobDetails[0].JobStatusMessage
						if message[0:8] == "finished" {
							fmt.Println(message)
							ticker.Stop()
							timeoutchan <- true
						}
					}
				} else {
					fmt.Printf("Querying dataset details failed with status code %v\n", resp.StatusCode)
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
}
