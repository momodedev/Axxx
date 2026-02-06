package dbmgr

import (
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs"
	"github.com/gogf/gf/os/glog"
)

// CreatePrdEventHubClient return a client of JobResultManager
func CreateconEventHubClient(connectionString, eventHubName string) (*azeventhubs.ProducerClient, error) {

	// Create a producer client to send messages to the event hub.
	producerClient, err := azeventhubs.NewProducerClientFromConnectionString(connectionString, eventHubName, nil)
	if err != nil {
		glog.Errorf("Cannot Connect to eventhub : %s", err.Error())
		return nil, err
	}

	return producerClient, nil
}
