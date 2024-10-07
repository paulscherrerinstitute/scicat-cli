package datasetUtils

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
)

type PublishedDataInfo struct {
	Doi      string   `json:"doi"`
	Title    string   `json:"title"`
	PidArray []string `json:"pidArray"`
}

func GetDatasetsOfPublication(client *http.Client, APIServer string, publishedDataId string) (datasetList []string, title string, doi string) {
	datasetList = make([]string, 0)

	var myurl = APIServer + "/PublishedData/" + url.QueryEscape(publishedDataId)
	// log.Println("Url:", myurl)

	resp, err := client.Get(myurl)
	if err != nil {
		log.Fatal("Get dataset details failed:", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == 200 {
		body, _ := ioutil.ReadAll(resp.Body)

		var respObj PublishedDataInfo
		err = json.Unmarshal(body, &respObj)
		if err != nil {
			log.Fatal(err)
		}

		log.Printf("Found the following datasets in published data\n")
		datasetList = respObj.PidArray
		title = respObj.Title
		doi = respObj.Doi
	} else {
		log.Printf("Statuscode:%v", resp.StatusCode)
	}
	return datasetList, title, doi
}
