package dbmgr

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs"
	"github.com/avast/retry-go"
	"github.com/gogf/gf/os/glog"
	"github.com/google/uuid"
	//"github.com/sethvargo/go-retry"
)

type AzTaskResultMgr struct {
	Client           *azeventhubs.ProducerClient
	AzTaskResultData AzTaskResultData
	ConnStr          string
	EhName           string
}

type AzTaskResultData struct {
	Id                 string `json:"Id"`
	TaskId             string `json:"TaskId"`
	TaskType           string `json:"TaskType"`
	NumberOfFiles      int    `json:"NumberOfFiles"`
	IsCompleted        bool   `json:"IsCompleted"`
	FinishedJob        int    `json:"FinishedJob"`
	FailedJob          int    `json:"FailedJob"`
	ExecutionStartTime string `json:"ExecutionStartTime"`
	CompletedTime      string `json:"CompletedTime"`
	TotalTransferBytes int    `json:"TotalTransferBytes"`
	TotalSize          int    `json:"TotalSize"`
	SkippedJob         int    `json:"SkippedJob"`
}

// GetTaskEventHubClient return a client of JobResultManager
// func GetTaskEventHubClient(connectionString, eventHubName string) *AzTaskResultMgr {

// 	// Create a producer client to send messages to the event hub.
// 	producerClient, err := CreateconEventHubClient(connectionString, eventHubName)
// 	if err != nil {
// 		return nil
// 	}
// 	return &AzTaskResultMgr{Client: producerClient, ConnStr: connectionString, EhName: eventHubName}
// }

func GetTaskEventHubClient(eventhubNamespace, eventHubName string) *AzTaskResultMgr {

	// Create a producer client to send messages to the event hub.
	//producerClient, err := CreateconEventHubClient(connectionString, eventHubName)
	cred, err := azidentity.NewManagedIdentityCredential(nil)
	if err != nil {
		panic(err)
	}
	// Create an Event Hub client
	producerClient, err := azeventhubs.NewProducerClient(eventhubNamespace, eventHubName, cred, nil)
	if err != nil {
		return nil
	}
	return &AzTaskResultMgr{Client: producerClient, ConnStr: eventhubNamespace, EhName: eventHubName}
}

// AzSetTaskResult update AzTaskResultData to eventhub
func (t *AzTaskResultMgr) SetAzTaskResultAsync(taskId string, taskType string, isComplete bool, skippedJob int, finishedJob int, failedJob int, totalTransferByte int, totalSize int64) error {
	//-j pk -t id
	t.AzTaskResultData.Id = uuid.New().String()
	t.AzTaskResultData.TaskId = taskId
	t.AzTaskResultData.TaskType = taskType

	t.AzTaskResultData.IsCompleted = isComplete
	t.AzTaskResultData.FinishedJob = finishedJob
	t.AzTaskResultData.FailedJob = failedJob
	t.AzTaskResultData.CompletedTime = time.Now().Format(time.RFC3339)
	t.AzTaskResultData.TotalTransferBytes = totalTransferByte
	t.AzTaskResultData.TotalSize = int(totalSize)
	t.AzTaskResultData.SkippedJob = skippedJob
	jsonData, err := json.Marshal(t.AzTaskResultData)
	//glog.Info(t.AzTaskResultData)
	glog.Debugf("Send Task Log to EventHub: %s", jsonData)

	if err != nil {
		glog.Errorf("Task Result Json Marshal error for task %s: %s", taskId, err.Error())
		return err
	}

	// Send the event data to the event hub.
	newBatchOptions := &azeventhubs.EventDataBatchOptions{
		// PartitionID: partitionID,

		// PartitionKey can be used to ensure that messages that have the same key
		// will go to the same partition without requiring your application to specify
		// that partition ID.
		//
		// PartitionKey: partitionKey,
	}

	batch, err := t.Client.NewEventDataBatch(context.TODO(), newBatchOptions)
	if err != nil {
		glog.Errorf("Cannot create batch for task %s: %s", taskId, err.Error())
		return err
	}

	// Add events to the batch. You can add events until the batch is full.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	retry.Do(
		func() error {
			if err := batch.AddEventData(&azeventhubs.EventData{Body: jsonData}, nil); err != nil {
				glog.Errorf("Cannot send batch for task %s: %s", taskId, err.Error())
				return err
			}
			return nil
		},
		retry.Delay(1*time.Second),
		retry.Attempts(3),
		retry.OnRetry(func(attempt uint, err error) {
			glog.Infof("Retrying task upload for task %s (attempt %d): %s", taskId, attempt, err.Error())
		}),
		retry.Context(ctx), // 传递上下文
	)

	// Send the batch.
	if err := t.Client.SendEventDataBatch(ctx, batch, nil); err != nil {
		glog.Errorf("Cannot send batch for task %s: %s", taskId, err.Error())
		return err
	}
	return nil
}

// CloseTaskEventHubClient close the client of JobResultManager
func (t *AzTaskResultMgr) CloseTaskEventHubClient() {
	t.Client.Close(context.Background())
}

func (t *AzTaskResultMgr) SetAzTaskResultOld(taskId string, taskType string, isComplete bool, skippedJob int, finishedJob int, failedJob int, totalTransferByte int, totalSize int64) error {
	t.AzTaskResultData.Id = uuid.New().String()
	t.AzTaskResultData.TaskId = taskId
	t.AzTaskResultData.TaskType = taskType
	t.AzTaskResultData.IsCompleted = isComplete
	t.AzTaskResultData.FinishedJob = finishedJob
	t.AzTaskResultData.FailedJob = failedJob
	t.AzTaskResultData.CompletedTime = time.Now().Format(time.RFC3339)
	t.AzTaskResultData.TotalTransferBytes = totalTransferByte
	t.AzTaskResultData.TotalSize = int(totalSize)
	t.AzTaskResultData.SkippedJob = skippedJob
	glog.Debugf("Send Task Log to EventHub: %+v", t.AzTaskResultData)
	jsonData, err := json.Marshal(t.AzTaskResultData)
	if err != nil {
		return fmt.Errorf("error marshalling task result data: %s", err)
	}
	attempt := 0
	maxAttempts := 3
	delay := time.Second
	for {
		attempt++
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
		defer cancel()
		batch, err := t.Client.NewEventDataBatch(ctx, nil)
		if err != nil {
			newClient := GetTaskEventHubClient(t.ConnStr, t.EhName).Client
			if attempt >= maxAttempts && newClient == nil {
				glog.Errorf("cannot create event data batch: %s", err)
				return fmt.Errorf("cannot create event data batch: %s", err)
			}
			t.Client = newClient
			time.Sleep(delay)
			delay *= 2
			continue

		}
		if err := batch.AddEventData(&azeventhubs.EventData{Body: jsonData}, nil); err != nil {
			glog.Errorf("cannot add event data: %s", err)
			return fmt.Errorf("cannot add event data: %s", err)
		}
		if err := t.Client.SendEventDataBatch(ctx, batch, nil); err != nil {
			if attempt >= maxAttempts {
				glog.Errorf("reach maximum attempt, last error: %s", err)
				return fmt.Errorf("reach maximum attempt, last error: %s", err)
			}
			time.Sleep(delay)
			delay *= 2
			continue
		}
		break
	}
	return nil
}

// func (t *AzTaskResultMgr) createBatchWithContext() (*azeventhubs.EventDataBatch, error) {
// 	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Minute)
// 	defer cancel()
// 	batch, err := t.Client.NewEventDataBatch(ctx, new(azeventhubs.EventDataBatchOptions))
// 	if err != nil {
// 		glog.Errorf("Failed to create event data batch: %s", err)
// 		return nil, err
// 	}
// 	return batch, nil
// }

// func (t *AzTaskResultMgr) trySendBatch(batch *azeventhubs.EventDataBatch) error {
// 	return retry.Do(
// 		func() error {
// 			ctx,
// 				cancel := context.WithTimeout(context.Background(), 30*time.Second)
// 			defer cancel()
// 			err := t.Client.SendEventDataBatch(ctx, batch, nil)
// 			// Handling specific network errors for retry.
// 			if isNetworkRelatedError(err) {

// 				t.Client = GetTaskEventHubClient(os.Getenv("DELTA_EVENTHUB_CONNSTR"), os.Getenv("DELTA_EVENTHUB_DELETETASK")).Client
// 				return nil
// 			}
// 			return nil
// 		}, retry.Attempts(5), retry.Delay(time.Second*30), retry.OnRetry(func(n uint, err error) {
// 			glog.Errorf("Attempt %d failed with error: %s. Retrying...", n+1, err)
// 		}))
// }

// func isNetworkRelatedError(err error) bool {
// 	if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
// 		glog.Debugf("Network Error: %s", netErr)
// 		return true
// 	} else if opErr, ok := err.(*net.OpError); ok {
// 		glog.Debugf("Operation Error: %s", opErr)
// 		return true
// 	}
// 	return false
// }

// func (t *AzTaskResultMgr) sendWithRetry(batch *azeventhubs.EventDataBatch) error {
// 	// Define backoff strategy
// 	backoff, err := retry.NewExponential(2 * time.Second)
// 	if err != nil {
// 		return err
// 	}
// 	// Add some randomness to prevent synchronization
// 	backoff = retry.WithJitter(0.2)(backoff)
// 	// Define retry policy
// 	retryFn := func(ctx context.Context, attempt uint) error {
// 		if err := t.Client.SendEventDataBatch(ctx, batch, nil); err != nil {
// 			glog.Errorf("Attempt %d: cannot send batch: %s", attempt, err)
// 			return retry.RetryableError(err)
// 		}
// 		return nil
// 	} // Execute the retryable function
// 	maxAttempts := uint(5)
// 	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
// 	defer cancel()
// 	if err := retry.Do(ctx, backoff, retryFn, retry.Attempts(maxAttempts)); err != nil {
// 		return err
// 	}
// 	return nil
// }

// SetAzTaskStartTimeWithRestAPI add task start time with RFC3339 format.
func SetAzTaskStartTime(AztaskResultData *AzTaskResultData, taskId string) {
	// Set Task Start Time
	AztaskResultData.TaskId = taskId
	AztaskResultData.ExecutionStartTime = time.Now().Format(time.RFC3339)

}

// AzSetTaskResult update AzTaskResultData to eventhub
// func (t *AzTaskResultMgr) SetAzTaskResult(taskId string, taskType string, isComplete bool, skippedJob int, finishedJob int, failedJob int, totalTransferByte int, totalSize int64) error {
// 	//-j pk -t id
// 	t.AzTaskResultData.Id = uuid.New().String()
// 	t.AzTaskResultData.TaskId = taskId
// 	t.AzTaskResultData.TaskType = taskType

// 	t.AzTaskResultData.IsCompleted = isComplete
// 	t.AzTaskResultData.FinishedJob = finishedJob
// 	t.AzTaskResultData.FailedJob = failedJob
// 	t.AzTaskResultData.CompletedTime = time.Now().Format(time.RFC3339)
// 	t.AzTaskResultData.TotalTransferBytes = totalTransferByte
// 	t.AzTaskResultData.TotalSize = int(totalSize)
// 	t.AzTaskResultData.SkippedJob = skippedJob
// 	jsonData, err := json.Marshal(t.AzTaskResultData)
// 	glog.Debug(t.AzTaskResultData)
// 	if err != nil {
// 		glog.Errorf("Task Result Json Marshal error: %s", err.Error())
// 	}

// 	// Send the event data to the event hub.
// 	newBatchOptions := &azeventhubs.EventDataBatchOptions{
// 		// PartitionID: partitionID,

// 		// PartitionKey can be used to ensure that messages that have the same key
// 		// will go to the same partition without requiring your application to specify
// 		// that partition ID.
// 		//
// 		// PartitionKey: partitionKey,
// 	}

// 	batch, err := t.Client.NewEventDataBatch(context.TODO(), newBatchOptions)

// 	if err != nil {
// 		glog.Errorf("Cannot create batch : %s", err.Error())
// 		return err
// 	}

// 	// Add events to the batch. You can add events until the batch is full.
// 	retry.Do(
// 		func() error {
// 			_, cancel := context.WithTimeout(context.Background(), 30*time.Second)
// 			defer cancel()
// 			if err := batch.AddEventData(&azeventhubs.EventData{Body: jsonData}, nil); err != nil {
// 				glog.Errorf("Cannot send batch : %s", err.Error())
// 				return err
// 			}

// 			return nil
// 		},
// 		retry.Delay(1*time.Second),
// 		retry.Attempts(3),
// 		retry.OnRetry(func(_ uint, err error) {
// 			glog.Infof("Retrying task upload task %s Info to eventhub : %s", taskId, err.Error())
// 		}),
// 	)

// 	// Send the batch.
// 	if err := t.Client.SendEventDataBatch(context.Background(), batch, nil); err != nil {
// 		glog.Errorf("Cannot send batch : %s", err.Error())
// 		return err
// 	}
// 	return nil
// }

// AzSetTaskResult update AzTaskResultData to eventhub
func (t *AzTaskResultMgr) SetAzTaskResultAsyncOld(taskId string, taskType string, isComplete bool, skippedJob int, finishedJob int, failedJob int, totalTransferByte int, totalSize int64) error {
	//-j pk -t id
	t.AzTaskResultData.Id = uuid.New().String()
	t.AzTaskResultData.TaskId = taskId
	t.AzTaskResultData.TaskType = taskType

	t.AzTaskResultData.IsCompleted = isComplete
	t.AzTaskResultData.FinishedJob = finishedJob
	t.AzTaskResultData.FailedJob = failedJob
	t.AzTaskResultData.CompletedTime = time.Now().Format(time.RFC3339)
	t.AzTaskResultData.TotalTransferBytes = totalTransferByte
	t.AzTaskResultData.TotalSize = int(totalSize)
	t.AzTaskResultData.SkippedJob = skippedJob
	jsonData, err := json.Marshal(t.AzTaskResultData)
	glog.Info(t.AzTaskResultData)
	if err != nil {
		glog.Errorf("Task Result Json Marshal error: %s", err.Error())
	}

	// Send the event data to the event hub.
	newBatchOptions := &azeventhubs.EventDataBatchOptions{
		// PartitionID: partitionID,

		// PartitionKey can be used to ensure that messages that have the same key
		// will go to the same partition without requiring your application to specify
		// that partition ID.
		//
		// PartitionKey: partitionKey,
	}

	batch, err := t.Client.NewEventDataBatch(context.TODO(), newBatchOptions)

	if err != nil {
		glog.Errorf("Cannot create batch : %s", err.Error())
		return err
	}

	// Add events to the batch. You can add events until the batch is full.
	retry.Do(
		func() error {
			_, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if err := batch.AddEventData(&azeventhubs.EventData{Body: jsonData}, nil); err != nil {
				glog.Errorf("Cannot send batch : %s", err.Error())
				return err
			}

			return nil
		},
		retry.Delay(1*time.Second),
		retry.Attempts(3),
		retry.OnRetry(func(_ uint, err error) {
			glog.Infof("Retrying task upload task %s Info to eventhub : %s", taskId, err.Error())
		}),
	)

	// Send the batch.
	if err := t.Client.SendEventDataBatch(context.Background(), batch, nil); err != nil {
		glog.Errorf("Cannot send batch : %s", err.Error())
		return err
	}
	return nil
}

// CloseTaskEventHubClient close the client of JobResultManager

func (t *AzTaskResultMgr) CloseTaskEventHubClientOld() {
	t.Client.Close(context.Background())
}
