package dbmgr_test

import (
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs"
	"github.com/Azure/azure-storage-azcopy/v10/dbmgr"
)

func TestSetAzTaskResult(t *testing.T) {
	// Initialize AzTaskResultMgr

	trm := dbmgr.AzTaskResultMgr{
		Client: &azeventhubs.ProducerClient{},
		AzTaskResultData: dbmgr.AzTaskResultData{
			Id:     "1",
			TaskId: "123",
			//PartitionKey:       "456",
			NumberOfFiles:      10,
			IsCompleted:        false,
			FinishedJob:        0,
			FailedJob:          0,
			ExecutionStartTime: "",
			CompletedTime:      "",
			TotalTransferBytes: 0,
			TotalSize:          0,
			SkippedJob:         0,
		},
	}

	// Call SetAzTaskResult
	err := trm.SetAzTaskResultAsync("123", "456", true, 0, 1, 0, 1024, 1024)

	// Check if err is nil
	if err != nil {
		t.Errorf("SetAzTaskResult failed")
	}
}
