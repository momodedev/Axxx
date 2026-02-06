package azblobmgr

import (
	"github.com/gogf/gf/os/glog"
	"testing"
	"time"
)

//func TestPreviewVerofAzBlobSDK(t *testing.T) {
//	azblobPreview.Version()
//	util.SetSysEnvTest()
//	azblobInfo := util.ParseAzBlobConnString(os.Getenv(config.AZ_BLOB_CONN_STRING))
//	cred, err := azblobPreview.NewSharedKeyCredential(azblobInfo["AccountName"], azblobInfo["AccountKey"])
//	if err != nil {
//		t.Error(err)
//	}
//
//	pipe := azblobPreview.NewPipeline(cred, azblobPreview.PipelineOptions{})
//	u, _ := url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net/%s", azblobInfo["AccountName"], os.Getenv(config.AZ_BLOB_CONTAINER)))
//
//	containerURL := azblobPreview.NewContainerURL(*u, pipe)
//
//	credential, err := azblobPreview.NewSharedKeyCredential(azblobInfo["AccountName"], azblobInfo["AccountKey"])
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	sasQueryParams, err := azblobPreview.AccountSASSignatureValues{
//		Protocol:      azblobPreview.SASProtocolHTTPS,
//		ExpiryTime:    time.Now().UTC().Add(48 * time.Hour),
//		Permissions:   azblobPreview.AccountSASPermissions{Read: true, List: true, Update: true, Write: true, Create: true, Add: true}.String(),
//		Services:      azblobPreview.AccountSASServices{Blob: true}.String(),
//		ResourceTypes: azblobPreview.AccountSASResourceTypes{Container: true, Object: true}.String(),
//	}.NewSASQueryParameters(credential)
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	qp := sasQueryParams.Encode()
//	urlToSendToSomeone := fmt.Sprintf("https://%s.blob.core.windows.net?%s", azblobInfo["AccountName"], qp)
//	glog.Info(urlToSendToSomeone)
//
//	glog.Info(containerURL.URL().Host)
//	blobURL := containerURL.NewBlobURL("lwazcopy/Img1.png")
//	glog.Info(blobURL.URL().Path)
//	ctx := context.TODO()
//
//	blobData, err := blobURL.GetProperties(ctx, azblobPreview.BlobAccessConditions{}, azblobPreview.ClientProvidedKeyOptions{})
//	if err != nil {
//		t.Error(err)
//	}
//	metadata := blobData.NewMetadata()
//	for k, v := range metadata {
//		fmt.Print(k + "=" + v + "\n")
//	}
//	testMap := make(map[string]string)
//	testMap["JustForMapTest"] = "IsThereAnything"
//	metadata["new"] = "nnn"
//	blobURL.SetMetadata(ctx, testMap, azblobPreview.BlobAccessConditions{}, azblobPreview.ClientProvidedKeyOptions{})
//}

func TestAzBlobMgr_IsJobNeedUploadToAzBlobByLastModifiedTime(t *testing.T) {
	lastModifyDate, _ := time.Parse("2006-01-02T15:04:05Z", "")
	s3LastModifyDate, _ := time.Parse("2006-01-02T15:04:05Z", "2022-11-29T08:16:11Z")
	if result := isTimeALateB(lastModifyDate, s3LastModifyDate); result {
		glog.Info("1")
	}
	glog.Info("0")
}

//func TestCreateNewQueue(t *testing.T) {
//	_url, err := url.Parse(fmt.Sprintf("https://%s.queue.core.windows.net/%s", storageAccountName, storageQueueName))
//	if err != nil {
//		glog.Error("Error parsing url: ", err)
//	}
//
//	credential, err := azqueue.NewSharedKeyCredential(storageAccountName, storageAccountKey)
//	if err != nil {
//		glog.Error("Error creating credentials: ", err)
//	}
//
//	queueUrl := azqueue.NewQueueURL(*_url, azqueue.NewPipeline(credential, azqueue.PipelineOptions{}))
//	queueUrl.Create(context.TODO(), nil)
//	msgUrl := queueUrl.NewMessagesURL()
//
//	newMessageContent := fmt.Sprintf("This will never expire %v", time.Now().Format(time.RFC3339))
//	_, err = msgUrl.Enqueue(context.TODO(), newMessageContent, 0, -time.Second)
//	if err != nil {
//		log.Fatal("Error adding message to queue: ", err)
//	}
//
//}
