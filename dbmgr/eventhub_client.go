package dbmgr

import (
	"bytes"
	"context"
	"encoding/json"
	"net/url"
	"sync"
	"time"

	"github.com/avast/retry-go"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs"
	"github.com/gogf/gf/os/glog"
)

type AzJobResultMgr struct {
	client    *azeventhubs.ProducerClient
	batchPool *sync.Pool
}

// func GetJobEventHubClient(connectionString, eventHubName string) *AzJobResultMgr {
// 	producerClient, err := azeventhubs.NewProducerClientFromConnectionString(connectionString, eventHubName, nil)
// 	if err != nil {
// 		glog.Error("Failed to create producer client:", err)
// 		return nil
// 	}
// 	return &AzJobResultMgr{
// 		client: producerClient, batchPool: &sync.Pool{
// 			New: func() interface{} {
// 				batch, err := producerClient.NewEventDataBatch(context.Background(), nil)
// 				if err != nil {
// 					glog.Error("Failed to create new batch:", err)
// 					return nil
// 				}
// 				return batch
// 			},
// 		},
// 	}
// }

func GetJobEventHubClient(eventhubNamespace, eventHubName string) *AzJobResultMgr {
	//producerClient, err := azeventhubs.NewProducerClientFromConnectionString(connectionString, eventHubName, nil)
	cred, err := azidentity.NewManagedIdentityCredential(nil)
	if err != nil {
		panic(err)
	}
	// Create an Event Hub client
	producerClient, err := azeventhubs.NewProducerClient(eventhubNamespace, eventHubName, cred, nil)
	if err != nil {
		glog.Error("Failed to create producer client:", err)
		return nil
	}
	return &AzJobResultMgr{
		client: producerClient, batchPool: &sync.Pool{
			New: func() interface{} {
				batch, err := producerClient.NewEventDataBatch(context.Background(), nil)
				if err != nil {
					glog.Error("Failed to create new batch:", err)
					return nil
				}
				return batch
			},
		},
	}
}

// type AzJobResultMgr struct {
// 	client *azeventhubs.ProducerClient
// }

type AzJobResultData struct {
	Id             string `json:"Id"`
	JobId          string `json:"JobId"`
	TaskId         string `json:"TaskId"`
	TaskType       string `json:"TaskType"`
	Message        string `json:"Message"`
	OperationTime  string `json:"OperationTime"`
	ModifyTime     string `json:"ModifyTime"`
	StorageClass   string `json:"StorageClass"`
	AccessTier     string `json:"AccessTier"`
	StatusCode     int    `json:"StatusCode"`
	Size           int64  `json:"Size"`
	SourceURL      string `json:"SourceUrl"`
	DestinationURL string `json:"DestinationURL"`
}

var jsonBufferPool = sync.Pool{New: func() interface{} { return new(bytes.Buffer) }}

// CreatePrdEventHubClient return a client of JobResultManager
// func GetJobEventHubClient(connectionString, eventHubName string) *AzJobResultMgr {

// 	// Create a producer client to send messages to the event hub.
// 	producerClient, err := CreateconEventHubClient(connectionString, eventHubName)
// 	if err != nil {
// 		return nil
// 	}
// 	return &AzJobResultMgr{client: producerClient}
// }

func decodeURL(encodedURL string) (string, error) {
	decodedURL, err := url.QueryUnescape(encodedURL)
	if err != nil {
		glog.Errorf("failed to decode URL: %+v", err)
		return "", err
	}
	return decodedURL, nil
}

func (j *AzJobResultMgr) SetAzJobResult(data AzJobResultData) bool {
	sUrl, errDecode := decodeURL(data.SourceURL)
	if errDecode == nil {

		data.SourceURL = sUrl
	}
	dUrl, errDecode := decodeURL(data.DestinationURL)
	if errDecode == nil {

		data.DestinationURL = dUrl
	}
	glog.Debugf("Send Job Log to EventHub: %+v", data)
	buf := jsonBufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	err := json.NewEncoder(buf).Encode(data)
	if err != nil {
		jsonBufferPool.Put(buf)
		return false
	}
	newBatchOptions := &azeventhubs.EventDataBatchOptions{}
	batch, err := j.client.NewEventDataBatch(context.TODO(), newBatchOptions)
	if err != nil {
		jsonBufferPool.Put(buf)
		glog.Errorf("Cannot create batch : %s", err.Error())
		return false
	}
	// create a batch object and add sample events to the batch
	retry.Do(func() error {

		glog.Debug("Add Job Log to Batch")

		_, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := batch.AddEventData(&azeventhubs.EventData{Body: buf.Bytes()}, nil); err != nil {
			glog.Errorf("Cannot send batch : %s", err.Error())
			return err
		}
		return nil
	}, retry.Delay(1*time.Second), retry.Attempts(3), retry.OnRetry(func(_ uint, err error) {
		glog.Infof("Retrying job upload Job %s Info to eventhub : %s", data.TaskId, err.Error())
	}))
	j.client.SendEventDataBatch(context.Background(), batch, nil)
	jsonBufferPool.Put(buf)
	return true
}

// SetAzJobResult update AzJobResultData to eventhub
// func (j *AzJobResultMgr) SetAzJobResult(AzjobResultData AzJobResultData) bool {

// 	jsonBytes, err := json.Marshal(AzjobResultData)
// 	if err != nil {
// 		return false
// 	}
// 	// Create a batch with one message.
// 	newBatchOptions := &azeventhubs.EventDataBatchOptions{
// 		// PartitionID: partitionID,

// 		// PartitionKey can be used to ensure that messages that have the same key
// 		// will go to the same partition without requiring your application to specify
// 		// that partition ID.
// 		//
// 		// PartitionKey: partitionKey,
// 	}

// 	batch, err := j.client.NewEventDataBatch(context.TODO(), newBatchOptions)

// 	if err != nil {
// 		glog.Errorf("Cannot create batch : %s", err.Error())
// 		return false
// 	}

// 	// create a batch object and add sample events to the batch
// 	retry.Do(
// 		func() error {
// 			_, cancel := context.WithTimeout(context.Background(), 30*time.Second)
// 			defer cancel()
// 			if err := batch.AddEventData(&azeventhubs.EventData{Body: jsonBytes}, nil); err != nil {
// 				glog.Errorf("Cannot send batch : %s", err.Error())
// 				return err
// 			}

// 			return nil
// 		},
// 		retry.Delay(1*time.Second),
// 		retry.Attempts(3),
// 		retry.OnRetry(func(_ uint, err error) {
// 			glog.Infof("Retrying job upload Job %s Info to eventhub : %s", AzjobResultData.TaskId, err.Error())
// 		}),
// 	)

// 	// Send the batch.

// 	if err := j.client.SendEventDataBatch(context.Background(), batch, nil); err != nil {
// 		glog.Errorf("Cannot send batch : %s", err.Error())
// 		return false
// 	}
// 	return true
// }

// SetAzBatchJobResult update AzJobResultData to eventhub with sync.WaitGroup and goroutine
func (j *AzJobResultMgr) SetAzBatchJobResult(AzjobResultData []AzJobResultData, wg *sync.WaitGroup) bool {

	// Create a batch with one message.
	newBatchOptions := &azeventhubs.EventDataBatchOptions{}

	batch, err := j.client.NewEventDataBatch(context.TODO(), newBatchOptions)

	if err != nil {
		glog.Errorf("Cannot create batch : %s", err.Error())
		return false
	}

	// BEGIN: ed8c6549bwf9
	for _, data := range AzjobResultData {
		wg.Add(1)
		go func(data AzJobResultData) {
			defer wg.Done()
			jsonBytes, err := json.Marshal(data)
			if err != nil {
				return
			}
			// Send the batch to the hub with retry
			retry.Do(
				func() error {
					_, cancel := context.WithTimeout(context.Background(), 30*time.Second)
					defer cancel()
					if err := batch.AddEventData(&azeventhubs.EventData{Body: jsonBytes}, nil); err != nil {
						glog.Errorf("Cannot send batch : %s", err.Error())
						return err
					}
					return nil
				},
				retry.Delay(1*time.Second),
				retry.Attempts(3),
				retry.OnRetry(func(_ uint, err error) {
					glog.Infof("Retrying job upload Job %s Info to eventhub : %s", data.TaskId, err.Error())
				}),
			)
		}(data)
	}
	// END: ed8c6549bwf9

	wg.Wait()

	if err := j.client.SendEventDataBatch(context.Background(), batch, nil); err != nil {
		glog.Errorf("Cannot send batch : %s", err.Error())
		return false
	}
	return true
}

// GetEventHubProperties
func (j *AzJobResultMgr) GetEventHubProperties(AzjobResultData AzJobResultData) string {

	eventHubProps, err := j.client.GetEventHubProperties(context.TODO(), nil)

	if err != nil {
		glog.Errorf("Cannot get eventhub properties : %s", err.Error())

	}

	for _, partitionID := range eventHubProps.PartitionIDs {

		return partitionID
	}

	return ""

}

// Close close the client
func (j *AzJobResultMgr) Close() {
	j.client.Close(context.Background())
}
