package datasetIngestor

import (
)

func ResetUpdatedMetaData(originalMap map[string]string, metaDataMap map[string]interface{}) {
	for k, v := range originalMap {
		metaDataMap[k] = v
	}
}
