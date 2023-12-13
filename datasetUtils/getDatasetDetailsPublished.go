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

const PUBLISHServer string = "doi2.psi.ch"

// get dataset details and filter by ownergroup

func GetDatasetDetailsPublished(client *http.Client, APIServer string, datasetList []string) ([]Dataset, []string) {

	outputDatasetDetails := make([]Dataset, 0)
	urls := make([]string, 0)
	sizeArray := make([]int, 0)
	numFilesArray := make([]int, 0)

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
			`"]}},"fields":{"pid":true,"sourceFolder":true,"size":true,"ownerGroup":true,"numberOfFiles":true}}`

		v := url.Values{}
		v.Set("filter", filter)
		v.Add("isPublished", "true")

		var myurl = APIServer + "/Datasets?" + v.Encode()
		// log.Println("Url:", myurl)

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
						outputDatasetDetails = append(outputDatasetDetails, datasetDetail)
						color.Set(color.FgGreen)
						log.Printf("%s %9d %v %v\n", datasetId, datasetDetail.Size/1024./1024., datasetDetail.OwnerGroup, datasetDetail.SourceFolder)
						color.Unset()
						//https: //doi2.psi.ch/datasets/das/work/p16/p16628/20181012_lungs/large_volume_360/R2-6/stitching/data_final_volume_fullresolution/
						url := "https://" + PUBLISHServer + "/datasets" + datasetDetail.SourceFolder
						urls = append(urls, url)
						sizeArray = append(sizeArray, datasetDetail.Size)
						numFilesArray = append(numFilesArray, datasetDetail.NumberOfFiles)
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
	return outputDatasetDetails, urls
}
