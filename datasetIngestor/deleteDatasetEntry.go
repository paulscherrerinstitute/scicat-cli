package datasetIngestor

import (
	"log"
	"net/http"
	"strings"
)

func DeleteDatasetEntry(client *http.Client, APIServer string, datasetId string, accessToken string) {
	req, err := http.NewRequest("DELETE", APIServer+"/Datasets/"+strings.Replace(datasetId, "/", "%2F", 1)+"?access_token="+accessToken, nil)
	// fmt.Printf("Request:%v\n",req)
	resp, err := client.Do(req)
	// fmt.Printf("resp %v %v\n",resp.Body,resp.StatusCode)
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()
}
