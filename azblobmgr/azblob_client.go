package azblobmgr

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	azblobPreview "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-storage-azcopy/v10/config"
	"github.com/Azure/azure-storage-queue-go/azqueue"
	"github.com/gogf/gf/os/glog"
)

type AzBlobMgr struct {
	ContainerClientDest *container.Client
	ContainerClientSrc  *container.Client
	QueueURL            azqueue.QueueURL
	AccountName         string
	SAS                 string
	ContainerNameSrc    string
	ContainerNameDest   string
	lock                sync.RWMutex
	sasCache            sync.Map
}

// GetOrCreateSAS retrieves a SAS token from cache or creates it if not present or expired.
// func (a *AzBlobMgr) GetOrCreateSAS(azblobInfo map[string]string) string {
// 	accountName := azblobInfo["AccountName"]
// 	if sas, ok := a.sasCache.Load(accountName); ok {
// 		return sas.(string)
// 	} else {
// 		a.lock.Lock()
// 		defer a.lock.Unlock()
// 		// double check in case it was created by another goroutine in the meantime
// 		if sas, ok := a.sasCache.Load(accountName); ok {
// 			return sas.(string)
// 		} else {
// 			a.GenerateSAS(azblobInfo)
// 			a.sasCache.Store(accountName, a.SAS)
// 			return a.SAS
// 		}
// 	}
// }

// existing GenerateSAS function here (omitted for brevity; consider enhancing for error handling and performance)

// func NewPipeline(credential azblobPreview.Credential) pipeline.Pipeline {
// 	// Set retry options
// 	retryOptions := azblobPreview.RetryOptions{
// 		Policy: azblobPreview.RetryPolicyExponential, MaxTries: 5, TryTimeout: 3 * time.Minute, RetryDelay: time.Second * 4, MaxRetryDelay: time.Minute,
// 	}
// 	// Set pipeline options
// 	pOptions := azblobPreview.PipelineOptions{
// 		Retry: retryOptions,
// 	}
// 	return azblobPreview.NewPipeline(credential, pOptions)

// }

func GetContextWithTimeout() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 2*time.Minute)
}

// GetAzBlobContainerClient return a AzBlobMgr struct, including Azure blob container client and Azure blob account name.
// func GetAzBlobContainerClient(azblobInfo map[string]string) *AzBlobMgr {
// 	cred, err := azblobPreview.NewSharedKeyCredential(azblobInfo["AccountName"], azblobInfo["AccountKey"])
// 	if err != nil {
// 		glog.Error("Cannot Connect to Azure Blob, Ple ase Check Key")
// 		panic("Cannot Connect to Azure Blob, Please Check Key")
// 	}
// 	//pipe := azblobPreview.NewPipeline(cred, azblobPreview.PipelineOptions{})
// 	pipe := NewPipeline(cred)
// 	u, _ := url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net/%s", azblobInfo["AccountName"], os.Getenv(config.AZ_BLOB_CONTAINER)))

// 	containerURL := azblobPreview.NewContainerURL(*u, pipe)
// 	return &AzBlobMgr{ContainerClient: &containerURL, AccountName: azblobInfo["AccountName"]}
// }

func GetAzBlobContainerClientDest(azblobInfo map[string]string) *AzBlobMgr {
	// Create a new service client with token credential
	credential, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		glog.Error("Cannot Connect to Azure Blob, Ple ase Check Key")
		panic("Cannot Connect to Azure Blob, Please Check Key")
	}
	client, err := azblobPreview.NewClient(azblobInfo["AccountURL"], credential, nil)
	if err != nil {
		glog.Error("Cannot Connect to Azure Blob, Ple ase Check Key")
		panic("Cannot Connect to Azure Blob, Please Check Key")
	}
	containerClient := client.ServiceClient().NewContainerClient(azblobInfo["ContainerName"])

	return &AzBlobMgr{ContainerClientDest: containerClient, AccountName: azblobInfo["AccountName"], ContainerNameDest: azblobInfo["ContainerName"]}
}

func GetAzBlobContainerClientSrc(azblobInfo map[string]string) *AzBlobMgr {
	// Create a new service client with token credential
	credential, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		glog.Error("Cannot Connect to Azure Blob, Ple ase Check Key")
		panic("Cannot Connect to Azure Blob, Please Check Key")
	}
	client, err := azblobPreview.NewClient(azblobInfo["AccountURL"], credential, nil)
	if err != nil {
		glog.Error("Cannot Connect to Azure Blob, Ple ase Check Key")
		panic("Cannot Connect to Azure Blob, Please Check Key")
	}
	containerClient := client.ServiceClient().NewContainerClient(azblobInfo["ContainerName"])

	return &AzBlobMgr{ContainerClientSrc: containerClient, AccountName: azblobInfo["AccountName"], ContainerNameSrc: azblobInfo["ContainerName"]}
}

// func (a *AzBlobMgr) GenerateSAS(azblobInfo map[string]string) (string, error) {
// 	credential, err := azblobPreview.NewSharedKeyCredential(azblobInfo["AccountName"], azblobInfo["AccountKey"])
// 	if err != nil {
// 		return "", err
// 	}
// 	now := time.Now().UTC()
// 	expiryTime := now.Add(48 * time.Hour)
// 	sasValues := azblobPreview.AccountSASSignatureValues{
// 		Protocol:    azblobPreview.SASProtocolHTTPS,
// 		StartTime:   now,
// 		ExpiryTime:  expiryTime,
// 		Permissions: "rwlacud",
// 		//Permissions:   "r",
// 		Services:      "b",
// 		ResourceTypes: "sco",
// 	}
// 	sasQueryParams, err := sasValues.NewSASQueryParameters(credential)
// 	if err != nil {
// 		return "", err
// 	}
// 	a.SAS = sasQueryParams.Encode()

// 	return a.SAS, nil
// }

//  // Define a struct to hold SAS details
//  type sasCacheItem struct {
// 	SASToken string
// 	ExpiresAt time.Time
// 	}
// 	// Global cache variable with a mutex for concurrency safety
// 	var sasCache = struct { sync.RWMutex items map[string]sasCacheItem }
// 	{items: make(map[string]sasCacheItem)}
// 			// GenerateSAS generate or retrieve a SAS from cache with current Azure blob settings.
// 			func (a *AzBlobMgr) GenerateSASNew(azblobInfo map[string]string) {
// 				cacheKey := azblobInfo["AccountName"] + azblobInfo["ContainerName"] + "SAS"
// 				// Lock for reading from the cache
// 				sasCache.RLock()
// 				if item, found := sasCache.items[cacheKey]; found && item.ExpiresAt.After(time.Now()) {
// 					sasCache.RUnlock() a.SAS = item.SASToken return
// 					}
// 					sasCache.RUnlock()
// 					// Lock for writing to the cache
// 					sasCache.Lock()
// 					defer sasCache.Unlock()
// 					// Double-checking if another goroutine has written to the cache
// 					if item, found := sasCache.items[cacheKey]; found && item.ExpiresAt.After(time.Now())
// 					{
// 						a.SAS = item.SASToken return
// 						} // Generate a new SAS
// 						credential, err := azblobPreview.NewSharedKeyCredential(azblobInfo["AccountName"], azblobInfo["AccountKey"])
// 						if err != nil {
// 							glog.Error(err)
// 							return
// 							}
// 							sasQueryParams, err := azblobPreview.AccountSASSignatureValues{
// 								Protocol: azblobPreview.SASProtocolHTTPS,
// 								StartTime: time.Now().UTC(),
// 								ExpiryTime: time.Now().UTC().Add(48 * time.Hour),
// 								Permissions: azblobPreview.AccountSASPermissions{Read: true, List: true, Update: true, Write: true, Create: true, Add: true, Delete: true}.String(),
// 								Services: azblobPreview.AccountSASServices{Blob: true}.String(),
// 								ResourceTypes: azblobPreview.AccountSASResourceTypes{Container: true, Object: true, Service: true}.String(),
// 								}.NewSASQueryParameters(credential)
// 								if err != nil {
// 									glog.Error(err) return
// 									}
// 									qp := sasQueryParams.Encode()
// 									a.SAS = qp
// 									// Cache the new SAS token
// 									sasCache.items[cacheKey] = sasCacheItem{
// 										SASToken: qp, ExpiresAt: time.Now().UTC().Add(48 * time.Hour),
// 										}
// 										// Logging the generation for debugging (optional)
// 										glog.Debugf("Generated SAS is : %s", a.SAS)
// 										}

// // GenerateSAS generate a SAS with current Azure blob settings.
// func (a *AzBlobMgr) GenerateSAS(azblobInfo map[string]string) {
// 	credential, err := azblobPreview.NewSharedKeyCredential(azblobInfo["AccountName"], azblobInfo["AccountKey"])
// 	if err != nil {
// 		glog.Error(err)
// 	}

// 	sasQueryParams, err := azblobPreview.AccountSASSignatureValues{
// 		Protocol:      azblobPreview.SASProtocolHTTPS,
// 		StartTime:     time.Now().UTC(),
// 		ExpiryTime:    time.Now().UTC().Add(48 * time.Hour),
// 		Permissions:   azblobPreview.AccountSASPermissions{Read: true, List: true, Update: true, Write: true, Create: true, Add: true, Delete: true}.String(),
// 		Services:      azblobPreview.AccountSASServices{Blob: true}.String(),
// 		ResourceTypes: azblobPreview.AccountSASResourceTypes{Container: true, Object: true, Service: true}.String(),
// 	}.NewSASQueryParameters(credential)
// 	if err != nil {
// 		glog.Error(err)
// 	}

// 	qp := sasQueryParams.Encode()

// 	a.SAS = qp

// 	//TODO DELETEME
// 	glog.Debugf("Generated SAS is : %s", a.SAS)
// }

// SetAzQueueClient generate a Azure queue client.
func (a *AzBlobMgr) SetAzQueueClient(azblobInfo map[string]string) {
	_url, err := url.Parse(fmt.Sprintf("https://%s.queue.core.windows.net/blobmetadata", azblobInfo["AccountName"]))
	if err != nil {
		glog.Error("Error parsing url: ", err)
	}

	credential, err := azqueue.NewSharedKeyCredential(azblobInfo["AccountName"], azblobInfo["AccountKey"])
	if err != nil {
		glog.Error("Error creating credentials: ", err)
	}

	queueUrl := azqueue.NewQueueURL(*_url, azqueue.NewPipeline(credential, azqueue.PipelineOptions{}))
	//_, err = queueUrl.Create(context.TODO(), nil)
	//if err != nil {
	//	glog.Errorf("Cannot Create Queue %s : %s", os.Getenv(config.AZ_BLOB_CONTAINER), err.Error())
	//}
	a.QueueURL = queueUrl
}

// AddBlobMetadataToQueue upload blob metadata into Azure queue.
func (a *AzBlobMgr) AddBlobMetadataToQueue(fileName, lastModifiedTime, versionId, storageClass, jobId, destURL string) {
	msgUrl := a.QueueURL.NewMessagesURL()
	msgContent := fmt.Sprintf("{\"BucketName\":\"%s\",\"FileName\":\"%s\",\"LastModifiedTime\":\"%s\",\"VersionId\":\"%s\",\"StorageClass\":\"%s\", \"RedisCache\":\"%s\", \"JobId\":\"%s\",\"DestinationURL\":\"%s\"}",
		os.Getenv(config.AZ_BLOB_CONTAINER), fileName, lastModifiedTime, versionId, storageClass, os.Getenv("RedisCache"), jobId, destURL)

	_, err := msgUrl.Enqueue(context.TODO(), msgContent, 0, -time.Second)
	if err != nil {
		glog.Error("Error adding message to queue: ", err)
	}
}

// SetBlobMetadata set blob metadata into Azure blob.
// func (a *AzBlobMgr) SetBlobMetadata(lastModifyDate string, versionID string, storageClass string, blobName string) {
// 	blobURL := a.ContainerClient.NewBlobURL(blobName)
// 	ctx := context.TODO()

// 	//blobData, err := blobURL.GetProperties(ctx, azblobPreview.BlobAccessConditions{}, azblobPreview.ClientProvidedKeyOptions{})
// 	var blobData *azblobPreview.BlobGetPropertiesResponse
// 	var err error
// 	err = retry.Do(func() error {
// 		blobData, err = blobURL.GetProperties(ctx, azblobPreview.BlobAccessConditions{}, azblobPreview.ClientProvidedKeyOptions{})
// 		if err != nil {
// 			return err
// 		}
// 		return nil
// 	}, retry.Attempts(3), retry.Delay(100*time.Microsecond))

// 	if err != nil {
// 		glog.Errorf("Cannot Get Blob %s Metadata", blobName)
// 	}
// 	metadata := blobData.NewMetadata()
// 	if lastModifyDate != "" {
// 		metadata["LastModifyTime"] = lastModifyDate
// 	}

// 	if versionID != "" {
// 		metadata["VersionId"] = versionID
// 	}

// 	if storageClass != "" {
// 		metadata["StorageClass"] = storageClass
// 	}

// 	err = retry.Do(func() error {
// 		_, err = blobURL.SetMetadata(ctx, metadata, azblobPreview.BlobAccessConditions{}, azblobPreview.ClientProvidedKeyOptions{})
// 		if err != nil {
// 			return err
// 		}
// 		return nil
// 	}, retry.Attempts(3), retry.Delay(100*time.Microsecond))

// 	if err != nil {
// 		glog.Error("Cannot Set Metadata for Blob %s", blobName)
// 	}
// }

// URLBuilder return a Azure blob URL with SAS.
func (a *AzBlobMgr) URLBuilder(containerName string, bucketName string) string {
	if containerName == "" {
		return fmt.Sprintf("https://%s.blob.core.windows.net/%s?%s", a.AccountName, bucketName, a.SAS)
	}
	return fmt.Sprintf("https://%s.blob.core.windows.net/%s?%s", a.AccountName, containerName, a.SAS)
}

// URLBuilder return a Azure blob URL with SAS.
func (a *AzBlobMgr) URLBuilderNoSAS(containerName string, bucketName string) string {
	if containerName == "" {
		return fmt.Sprintf("https://%s.blob.core.windows.net/%s?", a.AccountName, bucketName)
	}
	return fmt.Sprintf("https://%s.blob.core.windows.net/%s", a.AccountName, containerName)
}

// URLPrefixBuilder return a Azure blob URL without SAS.
func (a *AzBlobMgr) URLPrefixBuilder(containerName string, bucketName string) string {
	if containerName == "" {
		return fmt.Sprintf("https://%s.blob.core.windows.net/%s/", a.AccountName, bucketName)
	}
	return fmt.Sprintf("https://%s.blob.core.windows.net/%s/", a.AccountName, containerName)
}

// URLPrefixBuilder return a Azure blob URL without SAS.
func (a *AzBlobMgr) URLSourceBuilder(containerName string, srcAccountName string) string {
	if containerName == "" {
		return fmt.Sprintf("https://%s.blob.core.windows.net/%s/", srcAccountName, "")
	}
	return fmt.Sprintf("https://%s.blob.core.windows.net/%s/", srcAccountName, containerName)
}

// SrcURLBuilder return a Azure blob URL without SAS.
func (a *AzBlobMgr) SrcURLBuilder(containerName string, bucketName string, blobName string) string {
	if containerName == "" {
		return fmt.Sprintf("https://%s.blob.core.windows.net/%s/%s", a.AccountName, bucketName, blobName)
	}
	return fmt.Sprintf("https://%s.blob.core.windows.net/%s/%s", a.AccountName, containerName, blobName)
}

// IsJobNeedUploadToAzBlobByLastModifiedTime determine whether a file is needed to upload to Azure blob.
// func (a *AzBlobMgr) IsJobNeedUploadToAzBlobByLastModifiedTime(bucketName string, fileName string, s3LastModifytime string) bool {
// 	blobURL := a.ContainerClient.NewBlobURL(fileName)
// 	ctx := context.TODO()

// 	blobData, err := blobURL.GetProperties(ctx, azblobPreview.BlobAccessConditions{}, azblobPreview.ClientProvidedKeyOptions{})
// 	if err != nil {
// 		glog.Infof("File %s is not in Azure Blob, Rewrite Metadata Directly", bucketName+"/"+fileName)
// 		return true
// 	}
// 	metadata := blobData.NewMetadata()
// 	lastModifyDate, _ := time.Parse("2006-01-02T15:04:05Z", metadata["lastmodifytime"])
// 	s3LastModifyDate, _ := time.Parse("2006-01-02T15:04:05Z", s3LastModifytime)
// 	if isTimeALateB(s3LastModifyDate, lastModifyDate) {
// 		return true
// 	}
// 	return false
// }

// func isTimeALateB(a time.Time, b time.Time) bool {
// 	return !strings.HasPrefix(a.Sub(b).String(), "-")
// }

// func (a *AzBlobMgr) modifyBlobType(fileName string) bool {

// 	blobURL := a.ContainerClient.NewBlobURL(fileName)
// 	metadata := azblobPreview.Metadata{"newKey": "newValue"}
// 	_, err := blobURL.SetMetadata(context.Background(), metadata, azblobPreview.BlobAccessConditions{}, azblobPreview.ClientProvidedKeyOptions{})
// 	if err != nil {
// 		fmt.Printf("Failed to set metadata: %v\n", err)
// 		return false
// 	}
// 	fmt.Println("Metadata set on blob successfully!")
// 	return true
// }
