package google

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"cloud.google.com/go/storage"

	"github.com/stretchr/testify/assert"
)

// Change this as necessary to run tests
const bucket = "storage-package-test"
const targetURL = "http://mysafeinfo.com/api/data?list=englishmonarchs&format=json"
const filename = "english_monarchs_test.json"
const networkTemp = "networkTemp.json"
const putTemp = "putTemp.json"
const updateTemp = "updateTemp.json"
const zipTemp = "zipTemp.zip"

func unmarshal(data []byte) []map[string]interface{} {
	var result []map[string]interface{}
	err := json.Unmarshal(data, &result)
	if err != nil {
		panic(err)
	}
	return result
}

func testData() []byte {
	bytes, _ := json.Marshal([]map[string]interface{}{
		{
			"ID":      1,
			"Name":    "Edward the Elder",
			"Country": "United Kingdom",
			"House":   "House of Wessex",
			"Reign":   "899-925",
		},
		{
			"ID":      2,
			"Name":    "Athelstan",
			"Country": "United Kingdom",
			"House":   "House of Wessex",
			"Reign":   "925-940",
		},
		{
			"ID":      3,
			"Name":    "Edmund",
			"Country": "United Kingdom",
			"House":   "House of Wessex",
			"Reign":   "940-946",
		},
		{
			"ID":      4,
			"Name":    "Edred",
			"Country": "United Kingdom",
			"House":   "House of Wessex",
			"Reign":   "946-955",
		},
		{
			"ID":      5,
			"Name":    "Edwy",
			"Country": "United Kingdom",
			"House":   "House of Wessex",
			"Reign":   "955-959",
		},
	})
	return bytes
}

func TestMain(m *testing.M) {
	setup()
	code := m.Run()
	teardown()
	os.Exit(code)
}

// Try to fetch and save the test data from a remote server as networkTemp
func setup() {
	_, err := SaveNetworkFile(targetURL, bucket, networkTemp, nil)
	if err != nil {
		panic(err)
	}
	PutFile(bucket, updateTemp, testData())
}

func teardown() {
	ctx := context.Background()
	client, _ := storage.NewClient(ctx)
	defer client.Close()
	ctx, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()
	o := client.Bucket(bucket).Object(networkTemp)
	if err := o.Delete(ctx); err != nil {
		panic(err)
	}
	o2 := client.Bucket(bucket).Object(putTemp)
	if err := o2.Delete(ctx); err != nil {
		panic(err)
	}
	o3 := client.Bucket(bucket).Object(updateTemp)
	if err := o3.Delete(ctx); err != nil {
		panic(err)
	}
	o4 := client.Bucket(bucket).Object(zipTemp)
	if err := o4.Delete(ctx); err != nil {
		panic(err)
	}
}

func TestGetFile(t *testing.T) {
	file, _ := GetFile(bucket, filename)
	result := unmarshal(file)
	assert.Equal(t, result[0]["Reign"].(string), "899-925")
}

func TestPutFile(t *testing.T) {
	err := PutFile(bucket, putTemp, testData())
	assert.Nil(t, err)
	assert.True(t, Exists(bucket, putTemp))
	data, _ := GetFile(bucket, putTemp)
	assert.Equal(t, string(testData()), string(data))
}

func TestFilesAtPath(t *testing.T) {
	files, _ := FilesAtPath(bucket, filename)
	assert.Equal(t, 1, len(files))
	assert.Equal(t, files[0].FileName(), filename)
}

func TestProcessFile(t *testing.T) {
	var testField string
	ProcessFile(bucket, filename, func(data []byte) error {
		testField = unmarshal(data)[1]["Name"].(string)
		return nil
	})
	assert.Equal(t, "Athelstan", testField)
}

func TestProcessAndUpdateFile(t *testing.T) {
	err := ProcessAndUpdateFile(bucket, updateTemp, func(file []byte) ([]byte, error) {
		object := unmarshal(file)
		object[0]["Country"] = "Wessex"
		return json.Marshal(object)
	})
	assert.Nil(t, err)
	updatedObjectBytes, _ := GetFile(bucket, updateTemp)
	updatedObject := unmarshal(updatedObjectBytes)
	assert.Equal(t, "Wessex", updatedObject[0]["Country"].(string))

}

func TestZipAndUnZip(t *testing.T) {
	file1 := testData()
	file2 := testData()
	files := map[string][]byte{"file1": file1, "file2": file2}
	zippedBytes, _ := Zip(files)
	PutFile(bucket, zipTemp, zippedBytes)
	result, _ := GetFile(bucket, zipTemp)
	unZipped, _ := UnZip(result)
	assert.Equal(t, 2, len(unZipped))
	assert.Equal(t, file1, unZipped["file1"])
	assert.Equal(t, file2, unZipped["file2"])
}
