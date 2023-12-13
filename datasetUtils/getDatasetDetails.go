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

type Dataset struct {
	Pid           string
	SourceFolder  string
	Size          int
	OwnerGroup    string
	NumberOfFiles int
}

// get dataset details and filter by ownergroup

func GetDatasetDetails(client *http.Client, APIServer string, accessToken string, datasetList []string, ownerGroup string) []Dataset {

	outputDatasetDetails := make([]Dataset, 0)
	log.Println("Dataset ID                                         Size[MB]  Owner                      SourceFolder")
	log.Println("====================================================================================================")

	// split large request into chunks
	chunkSize := 100
	for i := 0; i < len(datasetList); i += chunkSize {
		end := i + chunkSize
		if end > len(datasetList) {
			end = len(datasetList)
		}

		var filter = `{"where":{"pid":{"inq":["` +
			strings.Join(datasetList[i:end], `","`) +
			`"]}},"fields":{"pid":true,"sourceFolder":true,"size":true,"ownerGroup":true}}`

		v := url.Values{}
		v.Set("filter", filter)
		v.Add("access_token", accessToken)

		var myurl = APIServer + "/Datasets?" + v.Encode()
		//log.Println("Url:", myurl)

		resp, err := client.Get(myurl)
		if err != nil {
			log.Fatal("Get dataset details failed:", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode == 200 {
			body, _ := ioutil.ReadAll(resp.Body)

			datasetDetails := make([]Dataset, 0)

			_ = json.Unmarshal(body, &datasetDetails)

			// verify if details were actually found for all available Datasets
			for _, datasetId := range datasetList[i:end] {
				detailsFound := false
				for _, datasetDetail := range datasetDetails {
					if datasetDetail.Pid == datasetId {
						detailsFound = true
						if ownerGroup == "" || ownerGroup == datasetDetail.OwnerGroup {
							outputDatasetDetails = append(outputDatasetDetails, datasetDetail)
							color.Set(color.FgGreen)
						}
						log.Printf("%s %9d %v %v\n", datasetId, datasetDetail.Size/1024./1024., datasetDetail.OwnerGroup, datasetDetail.SourceFolder)
						color.Unset()
						break
					}
				}
				if !detailsFound {
					color.Set(color.FgRed)
					log.Printf("Dataset %s no infos found in catalog - will not be copied !\n", datasetId)
					color.Unset()
				}
			}
		} else {
			log.Printf("Querying dataset details failed with status code %v\n", resp.StatusCode)
		}
	}
	return outputDatasetDetails
}
