package dbmgr_test

import (
	"testing"

	"github.com/Azure/azure-storage-azcopy/v10/dbmgr"
)

func TestCreateconEventHubClient(t *testing.T) {
	// Initialize test variables
	connectionString := "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=1234567890"
	eventHubName := "test-eventhub"

	// Call CreateconEventHubClient
	client, err := dbmgr.CreateconEventHubClient(connectionString, eventHubName)

	// Check if client is nil and err is not nil
	if client == nil || err != nil {
		t.Errorf("CreateconEventHubClient failed")
	}
}
