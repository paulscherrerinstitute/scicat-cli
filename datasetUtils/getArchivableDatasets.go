package datasetUtils

import (
	"encoding/json"
	"github.com/fatih/color"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strings"
)

type DatasetInfo struct {
	Pid          string `json:"pid"`
	SourceFolder string `json:"sourceFolder"`
	Size         int    `json:"size"`
}
type QueryResult []DatasetInfo

// function that assembles the datasetIds to be fetched in chunks
// see https://blog.golang.org/slices for explanation why datasetList slice should be a return parameter

func addResult(client *http.Client, APIServer string, filter string, accessToken string, datasetList []string) []string {
	v := url.Values{}
	v.Set("filter", filter)
	v.Add("access_token", accessToken)

	var myurl = APIServer + "/Datasets?" + v.Encode()
	//fmt.Println("Url:", myurl)

	resp, err := client.Get(myurl)
	if err != nil {
		log.Fatal("Get dataset details failed:", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == 200 {
		body, _ := ioutil.ReadAll(resp.Body)

		var respObj QueryResult
		err = json.Unmarshal(body, &respObj)
		if err != nil {
			log.Fatal(err)
		}

		if len(respObj) > 0 {
			log.Printf("Found the following datasets in state archivable: (size=0 datasets are removed)")
			var item DatasetInfo
			for _, item = range respObj {
				if item.Size > 0 {
					log.Printf("Folder: %v, size: %v, PID: %v", item.SourceFolder, item.Size, item.Pid)
					datasetList = append(datasetList, item.Pid)
				} else {
					color.Set(color.FgRed)
					log.Printf("Folder: %v, size: %v, PID: %v will be ignored !", item.SourceFolder, item.Size, item.Pid)
					color.Unset()
				}
			}
		}
	} else {
		log.Printf("Statuscode:%v", resp.StatusCode)
	}
	return datasetList
}

func GetArchivableDatasets(client *http.Client, APIServer string, ownerGroup string, inputdatasetList []string, accessToken string) (datasetList []string) {
	datasetList = make([]string, 0)

	filter := ""
	if ownerGroup != "" {
		filter = `{"where":{"ownerGroup":"` + ownerGroup + `","datasetlifecycle.archivable":true},"fields": {"pid":1,"size":1,"sourceFolder":1}}`
		datasetList = addResult(client, APIServer, filter, accessToken, datasetList)
	} else {
		// split large request into chunks
		chunkSize := 100
		for i := 0; i < len(inputdatasetList); i += chunkSize {
			end := i + chunkSize
			if end > len(inputdatasetList) {
				end = len(inputdatasetList)
			}
			quotedList := strings.Join(inputdatasetList[i:end], "\",\"")
			filter = `{"where":{"pid":{"inq":["` + quotedList + `"]},"datasetlifecycle.archivable":true},"fields": {"pid":1,"size":1,"sourceFolder":1}}`
			datasetList = addResult(client, APIServer, filter, accessToken, datasetList)
		}
	}
	return datasetList
}
