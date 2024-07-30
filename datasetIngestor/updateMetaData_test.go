package datasetIngestor

import (
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"
)

func TestGetAVFromPolicy(t *testing.T) {
	// Test case 1: No policies available
	ts1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`[]`)) // empty policy list
	}))
	defer ts1.Close()

	client := ts1.Client()

	level := getAVFromPolicy(client, ts1.URL, map[string]string{"accessToken": "testToken"}, "testOwner")

	if level != "low" {
		t.Errorf("Expected level to be 'low', got '%s'", level)
	}

	// Test case 2: Policies available
	ts2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`[{"TapeRedundancy": "medium", "AutoArchive": false}]`)) // policy list with one policy
	}))
	defer ts2.Close()

	client = ts2.Client()

	level = getAVFromPolicy(client, ts2.URL, map[string]string{"accessToken": "testToken"}, "testOwner")

	if level != "medium" {
		t.Errorf("Expected level to be 'medium', got '%s'", level)
	}
}

// Check whether `UpdateMetaData` correctly updates the metaDataMap
func TestUpdateMetaData(t *testing.T) {
	// Create a mock server
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`[{"TapeRedundancy": "medium", "AutoArchive": false}]`)) // policy list with one policy
	}))
	defer ts.Close()

	// Create a test client
	client := ts.Client()

	// Define test parameters
	APIServer := ts.URL // Use the mock server's URL

	user := map[string]string{"accessToken": "testToken"}
	originalMap := map[string]string{}
	metaDataMap := map[string]interface{}{
		"creationTime": DUMMY_TIME,
		"ownerGroup":   DUMMY_OWNER,
		"type":         "raw",
		"endTime":      DUMMY_TIME,
	}
	startTime := time.Now()
	endTime := startTime.Add(time.Hour)
	owner := "testOwner"
	tapecopies := 1

	// Call the function
	UpdateMetaData(client, APIServer, user, originalMap, metaDataMap, startTime, endTime, owner, tapecopies)

	// Check results
	if metaDataMap["creationTime"] != startTime {
		t.Errorf("Expected creationTime to be '%v', got '%v'", startTime, metaDataMap["creationTime"])
	}
	if metaDataMap["ownerGroup"] != owner {
		t.Errorf("Expected ownerGroup to be '%s', got '%s'", owner, metaDataMap["ownerGroup"])
	}
	if metaDataMap["endTime"] != endTime {
		t.Errorf("Expected endTime to be '%v', got '%v'", endTime, metaDataMap["endTime"])
	}
	if metaDataMap["license"] != "CC BY-SA 4.0" {
		t.Errorf("Expected license to be 'CC BY-SA 4.0', got '%s'", metaDataMap["license"])
	}
	if metaDataMap["isPublished"] != false {
		t.Errorf("Expected isPublished to be 'false', got '%v'", metaDataMap["isPublished"])
	}
	if _, ok := metaDataMap["classification"]; !ok {
		t.Errorf("Expected classification to be set, got '%v'", metaDataMap["classification"])
	}
}

// Check if updateFieldIfDummy correctly updates the metaDataMap and originalMap when the field is a dummy value.
func TestUpdateFieldIfDummy(t *testing.T) {
	// Define test parameters
	metaDataMap := map[string]interface{}{
		"testField": "DUMMY",
	}
	originalMap := map[string]string{}
	fieldName := "testField"
	dummyValue := "DUMMY"
	newValue := "newValue"

	// Call the function
	updateFieldIfDummy(metaDataMap, originalMap, fieldName, dummyValue, newValue)

	// Check results
	if metaDataMap[fieldName] != newValue {
		t.Errorf("Expected %s to be '%v', got '%v'", fieldName, newValue, metaDataMap[fieldName])
	}
	if originalMap[fieldName] != dummyValue {
		t.Errorf("Expected original %s to be '%v', got '%v'", fieldName, dummyValue, originalMap[fieldName])
	}
}

// Check if addFieldIfNotExists correctly adds a field to the metaDataMap if it does not already exist.
func TestAddFieldIfNotExists(t *testing.T) {
	// Define test parameters
	metaDataMap := map[string]interface{}{}
	fieldName := "testField"
	value := "testValue"

	// Call the function
	addFieldIfNotExists(metaDataMap, fieldName, value)

	// Check results
	if metaDataMap[fieldName] != value {
		t.Errorf("Expected %s to be '%v', got '%v'", fieldName, value, metaDataMap[fieldName])
	}
}

// Check if updateClassificationField correctly updates the classification field in the metaDataMap.
func TestUpdateClassificationField(t *testing.T) {
	// Create a mock server
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`[{"TapeRedundancy": "medium", "AutoArchive": false}]`)) // policy list with one policy
	}))
	defer ts.Close()

	// Create a test client
	client := ts.Client()

	// Define test parameters
	APIServer := ts.URL // Use the mock server's URL
	user := map[string]string{"accessToken": "testToken"}
	metaDataMap := map[string]interface{}{
		"ownerGroup": "testOwner",
	}
	tapecopies := 1

	expectedValue1 := "IN=medium,AV=low,CO=low"
	expectedValue2 := "IN=medium,AV=medium,CO=low"

	// Call the function
	updateClassificationField(client, APIServer, user, metaDataMap, tapecopies)

	// Check results
	if _, ok := metaDataMap["classification"]; !ok {
		t.Errorf("Expected classification to be set, got '%v'", metaDataMap["classification"])
	} else if metaDataMap["classification"] != expectedValue1 {
		t.Errorf("Expected classification to be '%v', got '%v'", expectedValue1, metaDataMap["classification"])
	}

	// Change tapecopies to 2 and call the function again
	tapecopies = 2
	updateClassificationField(client, APIServer, user, metaDataMap, tapecopies)

	// Check results
	if _, ok := metaDataMap["classification"]; !ok {
		t.Errorf("Expected classification to be set, got '%v'", metaDataMap["classification"])
	} else if metaDataMap["classification"] != expectedValue2 {
		t.Errorf("Expected classification to be '%v', got '%v'", expectedValue2, metaDataMap["classification"])
	}
}

// checks that the function correctly updates the "AV" field in the classification string
func TestGetUpdatedClassification(t *testing.T) {
	metaDataMap := map[string]interface{}{
		"classification": "IN=medium,AV=low,CO=low",
	}
	av := "AV=medium"

	expected := []string{"IN=medium", "AV=medium", "CO=low"}
	result := getUpdatedClassification(metaDataMap, av)

	if !reflect.DeepEqual(result, expected) {
		t.Errorf("getUpdatedClassification() = %v; want %v", result, expected)
	}
}

func TestGetAVValue(t *testing.T) {
	tapecopies1 := 1
	tapecopies2 := 2

	expected1 := AVLow
	expected2 := AVMedium

	result1 := getAVValue(tapecopies1)
	result2 := getAVValue(tapecopies2)

	if result1 != expected1 {
		t.Errorf("getAVValue() with tapecopies = 1 = %v; want %v", result1, expected1)
	}

	if result2 != expected2 {
		t.Errorf("getAVValue() with tapecopies = 2 = %v; want %v", result2, expected2)
	}
}

func TestUpdateAVField(t *testing.T) {
	tapecopies1 := 1
	tapecopies2 := 2

	metaDataMap1 := map[string]interface{}{
		"classification": "IN=medium,AV=medium,CO=low",
	}

	metaDataMap2 := map[string]interface{}{
		"classification": "IN=medium,AV=low,CO=low",
	}

	expected1 := "IN=medium,AV=low,CO=low"
	expected2 := "IN=medium,AV=medium,CO=low"

	updateAVField(metaDataMap1, tapecopies1)
	updateAVField(metaDataMap2, tapecopies2)

	if metaDataMap1["classification"] != expected1 {
		t.Errorf("updateAVField() with tapecopies = 1 = %v; want %v", metaDataMap1["classification"], expected1)
	}

	if metaDataMap2["classification"] != expected2 {
		t.Errorf("updateAVField() with tapecopies = 2 = %v; want %v", metaDataMap2["classification"], expected2)
	}
}
