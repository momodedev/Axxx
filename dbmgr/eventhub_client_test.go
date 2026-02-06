package dbmgr_test

import (
	"testing"

	"github.com/Azure/azure-storage-azcopy/v10/dbmgr"
)

func TestGetEventHubProperties(t *testing.T) {
	// Initialize AzJobResultMgr
	eventHubConnString := "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=1234567890"
	eventHubName := "test-eventhub"

	jrm := dbmgr.GetJobEventHubClient(eventHubConnString, eventHubName)

	// Create a sample AzJobResultData

	jrd := dbmgr.AzJobResultData{
		Id:       "1",
		JobId:    "123",
		TaskId:   "456",
		TaskType: "TaskType",
		//ContainerName:  "test-bucket",
		Message:        "test message",
		OperationTime:  "2022-01-01T00:00:00Z",
		ModifyTime:     "2022-01-01T00:00:00Z",
		StorageClass:   "STANDARD",
		StatusCode:     200,
		Size:           1024,
		SourceURL:      "https://source.com",
		DestinationURL: "https://destination.com",
	}
	// Call GetEventHubProperties
	result := jrm.GetEventHubProperties(jrd)
	// Check if result is "0"
	if result != "0" {
		t.Errorf("GetEventHubProperties failed")
	}
}
