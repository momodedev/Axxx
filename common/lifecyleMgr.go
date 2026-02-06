package common

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"reflect"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/keyvault/azsecrets"
	"github.com/Azure/azure-storage-azcopy/v10/azblobmgr"
	"github.com/Azure/azure-storage-azcopy/v10/config"
	"github.com/Azure/azure-storage-azcopy/v10/dbmgr"
	"github.com/Azure/azure-storage-azcopy/v10/util"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/gogf/gf/os/glog"
	"github.com/google/uuid"
	"github.com/minio/minio-go"
	goredis "github.com/redis/go-redis/v9"

	gcpUtils "cloud.google.com/go/storage"
	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"google.golang.org/api/googleapi"
)

const (
	Copy = "copy"

	Remove = "remove"

	Redis_Host = "REDIS_HOST"

	Redis_PWD = "REDIS_PWD"

	Full_EventHub_ConnStr = "FULL_EVENTHUB_CONNSTR"
	Full_EventHub_Task    = "FULL_EVENTHUB_TASK"
	Full_EventHub_Job     = "FULL_EVENTHUB_JOB"

	Delta_EventHub_ConnStr    = "DELTA_EVENTHUB_CONNSTR"
	Delta_EventHub_UpsertTask = "DELTA_EVENTHUB_UPSERTTASK"
	Delta_EventHub_UpsertJob  = "DELTA_EVENTHUB_UPSERTJOB"

	Delta_EventHub_DeleteTask = "DELTA_EVENTHUB_DELETETASK"
	Delta_EventHub_DeleteJob  = "DELTA_EVENTHUB_DELETEJOB"

	Debug_Mode = "DEBUG_MODE"

	Job_Status_Success = 1

	Job_Status_Failed = -1

	Job_Status_SkippedSuccess = -3

	Job_Delete_Message_SkippedSuccess = "File exists in source, skipped"

	Job_Delete_Message_SkippedFail = "File is not existing in target, failed"

	Job_Archived_Message_SkippedSuccess = "File is archived in SA, skipped"

	Job_BlobNotFound_Message_SkippedSuccess = "File is not existing in source, skipped"

	Err_Message_404 = "404 File did not found"

	Err_Message_403 = "403 This request is not authorized to perform this operation"

	Task_Type = "TASK_TYPE"

	Task_Max_File = 1000

	Error_Http_Request_Failed = "HTTP request failed"

	Error_Authorization_Failure = "AuthorizationFailure"

	Error_BlobNotFound = "BlobNotFound"

	Error_BlobNotFound404 = "404"

	Error_Request_Error = "RequestError"

	Error_Forbidden = "Forbidden"

	Error_NotFound = "NotFound"

	Aliyun_OSS = "aliyuncs.com"

	Azure_Blob = "blob.core.windows.net"

	//b2b
	AWS_S3_FORMAT          = "https://%s.s3.%s.amazonaws.com"
	AWS_ENDPOINT_FORMAT    = "https://s3.%s.amazonaws.com"
	ALIYUN_OSS_FORMAT      = "https://%s.oss-%s.aliyuncs.com"
	ALIYUN_ENDPOINT_FORMAT = "https://oss-%s.aliyuncs.com"
)

// only one instance of the formatter should exist
var lcm = func() (lcmgr *lifecycleMgr) {
	lcmgr = &lifecycleMgr{
		msgQueue:             make(chan outputMessage, 1000),
		progressCache:        "",
		cancelChannel:        make(chan os.Signal, 1),
		e2eContinueChannel:   make(chan struct{}),
		e2eAllowOpenChannel:  make(chan struct{}),
		outputFormat:         EOutputFormat.Text(), // output text by default
		logSanitizer:         NewAzCopyLogSanitizer(),
		inputQueue:           make(chan userInput, 1000),
		allowCancelFromStdIn: false,
		allowWatchInput:      false,
		closeFunc:            func() {}, // noop since we have nothing to do by default
		waitForUserResponse:  make(chan bool),
		msgHandlerChannel:    make(chan *LCMMsg),
	}

	// kick off the single routine that processes output
	go lcmgr.processOutputMessage()

	// and process input
	go lcmgr.watchInputs()

	// Check if need to do CPU profiling, and do CPU profiling accordingly when azcopy life start.
	lcmgr.checkAndStartCPUProfiling()

	return
}()

// create a public interface so that consumers outside of this package can refer to the lifecycle manager
// but they would not be able to instantiate one
type LifecycleMgr interface {
	Init(OutputBuilder)                                          // let the user know the job has started and initial information like log location
	Progress(OutputBuilder)                                      // print on the same line over and over again, not allowed to float up
	Exit(OutputBuilder, ExitCode)                                // indicates successful execution exit after printing, allow user to specify exit code
	Info(string)                                                 // simple print, allowed to float up
	Dryrun(OutputBuilder)                                        // print files for dry run mode
	Error(string)                                                // indicates fatal error, exit after printing, exit code is always Failed (1)
	Prompt(message string, details PromptDetails) ResponseOption // ask the user a question(after erasing the progress), then return the response
	SurrenderControl()                                           // give up control, this should never return
	InitiateProgressReporting(WorkController)                    // start writing progress with another routine
	AllowReinitiateProgressReporting()                           // allow re-initiation of progress reporting for followup job
	GetEnvironmentVariable(EnvironmentVariable) string           // get the environment variable or its default value
	ClearEnvironmentVariable(EnvironmentVariable)                // clears the environment variable
	SetOutputFormat(OutputFormat)                                // change the output format of the entire application
	EnableInputWatcher()                                         // depending on the command, we may allow user to give input through Stdin
	EnableCancelFromStdIn()                                      // allow user to send in `cancel` to stop the job
	AddUserAgentPrefix(string) string                            // append the global user agent prefix, if applicable
	E2EAwaitContinue()                                           // used by E2E tests
	E2EAwaitAllowOpenFiles()                                     // used by E2E tests
	E2EEnableAwaitAllowOpenFiles(enable bool)                    // used by E2E tests
	RegisterCloseFunc(func())
	SetForceLogging()
	IsForceLoggingDisabled() bool
	DownloadToTempPath() bool
	MsgHandlerChannel() <-chan *LCMMsg
	ReportAllJobPartsDone()
	SetOutputVerbosity(mode OutputVerbosity)
	LoadConfigurationFileFromYaml(taskID string, jobID string)
	SetOverwriteIfNew(overwriteValue string)
	GetOverwriteIfNew() bool
	GetJobDataFromjsonfileS3(taskId string, jobId string) []string
	GetJobDataFromjsonfileB2B(taskId string, jobId string) []string
	GetJobDataFromjsonfileGCS(taskId string, jobId string) []string
	ConvertRedisDataToMinioObjectInfo(bucketName string, objKey string) (minio.ObjectInfo, error)
	//620 SubmitTaskResultToCosmos(totalJobs int, skippedJob int, finishedJob int, failedJob int, totalTransferBytes int)
	GetJobDataFromMapWithKeyB2B(key string) dbmgr.JobDataB2B
	GetJobDataFromMapWithKeyS3(key string) dbmgr.JobDataS3
	deleteRedisJobsWhenFinished()
	//620 SetSingleJobDataToCosmos(fileName string, status int, message string, statusCode int)
	//SubmitJobResult(builder func(format OutputFormat) string, exitCode ExitCode)
	//620 SetJobCosmosMgr()
	SetTaskExecutionStartTime()
	GetKeyVault()
	GetAzureKeys(jobId string)
	//620
	SubmitTaskResultToEventHubB2B(totalJobs int, skippedJob int, finishedJob int, failedJob int, totalTransferBytes int)
	SetSingleJobDataToEventhubB2B(fileName string, status int, message string, statusCode int)

	SubmitTaskResultToEventHubS3(totalJobs int, skippedJob int, finishedJob int, failedJob int, totalTransferBytes int)
	SetSingleJobDataToEventhubS3(fileName string, status int, message string, statusCode int)

	SubmitJobLogBeforeCmdS3(jobItem dbmgr.JobDataS3, message string, jobStatus int)

	SubmitJobLogBeforeCmdB2B(jobItem dbmgr.JobDataB2B, message string, jobStatus int)

	//for del
	getTaskType() string

	CacheAzblobCre(azblobcre azblob.Credential)

	GetCacheAzblobCre() azblob.Credential

	CreateCuOauthTokenManager(tmanager *UserOAuthTokenManager)

	GetCuOauthTokenManager() *UserOAuthTokenManager
}

func GetLifecycleMgr() LifecycleMgr {
	return lcm
}

// single point of control for all outputs
type lifecycleMgr struct {
	msgQueue              chan outputMessage
	progressCache         string // useful for keeping job progress on the last line
	cancelChannel         chan os.Signal
	doneChannel           chan bool
	e2eContinueChannel    chan struct{}
	e2eAllowOpenChannel   chan struct{}
	waitEverCalled        int32
	outputFormat          OutputFormat
	logSanitizer          pipeline.LogSanitizer
	inputQueue            chan userInput // msgs from the user
	allowWatchInput       bool           // accept user inputs and place then in the inputQueue
	allowCancelFromStdIn  bool           // allow user to send in 'cancel' from the stdin to stop the current job
	e2eAllowAwaitContinue bool           // allow the user to send 'continue' from stdin to start the current job
	e2eAllowAwaitOpen     bool           // allow the user to send 'open' from stdin to allow the opening of the first file
	closeFunc             func()         // used to close logs before exiting
	disableSyslog         bool
	waitForUserResponse   chan bool
	msgHandlerChannel     chan *LCMMsg
	OutputVerbosityType   OutputVerbosity

	JobDataRedisClient *goredis.Client
	FileSliceS3        map[string]dbmgr.JobDataS3
	FileSliceB2B       map[string]dbmgr.JobDataB2B
	YamlConfig         *config.Config
	TaskType           string
	TaskIDFromParam    string
	JobIDFromParam     string
	//620	JobResultData      []dbmgr.JobResultData
	OverwriteIfNew bool
	AzBlobMgrSrc   *azblobmgr.AzBlobMgr

	//B2B
	AzBlobMgrDest *azblobmgr.AzBlobMgr

	//620 CosmosJobMgr      *dbmgr.JobResultMgr
	KeyVaultClient  *azsecrets.Client
	SourceURLPrefix string
	AzBlobURLPrefix string
	CosmosPKRange   int
	//620
	AzTaskResultMgr *dbmgr.AzTaskResultMgr
	AzJobResultMgr  *dbmgr.AzJobResultMgr
	//630
	DebugMode int
	//630
	SkippedDelNum int

	AzBlobContainer string

	azblobCre azblob.Credential

	cuOAuthTokenManager *UserOAuthTokenManager
}

// CreateCuOauthTokenManager implements LifecycleMgr.
func (lcm *lifecycleMgr) CreateCuOauthTokenManager(tmanager *UserOAuthTokenManager) {
	lcm.cuOAuthTokenManager = tmanager
}

// GetCuOauthTokenManager implements LifecycleMgr.
func (lcm *lifecycleMgr) GetCuOauthTokenManager() *UserOAuthTokenManager {
	return lcm.cuOAuthTokenManager
}

type userInput struct {
	timeReceived time.Time
	content      string
}

// GetJobDataFromRedis read task detail information from redis.
// Add detail information to map lcm.FileSlice.
// Add transfer file names into temp file (txt).
// Rewrite "-t -j" args into Azcopy args.
// Return rewrote Azcopy args.
func (lcm *lifecycleMgr) GetJobDataFromjsonfileB2B(taskId string, jobId string) []string {
	var sourceSaName, sourceUrlPresfix, sourceUrl, toTier string
	//sourceBCName = os.Getenv("SOURCE_BUCKET")
	azBlobContainer := os.Getenv("AZ_BLOB_CONTAINER")
	result := []string{}
	//620 rdb := lcm.JobDataRedisClient

	//THIS IS NO LONGER NEEDED
	//if lcm.JobDataRedisClient == nil {
	//	rdb = goredis.NewClient(&goredis.Options{
	//		Addr:      lcm.YamlConfig.REDIS_HOST,
	//		Password:  lcm.YamlConfig.REDIS_PSW,
	//		DB:        0,
	//		TLSConfig: &tls.Config{MinVersion: tls.VersionTLS12},
	//	})
	//	lcm.JobDataRedisClient = rdb
	//}

	//620 newJob := []dbmgr.JobData{}

	//620 jobs, err := rdb.Get(context.TODO(), lcm.TaskIDFromParam).Result()
	//
	//if err != nil {
	//	glog.Errorf("Error During Get Job Data From Redis %s", err)
	//	os.Exit(-1)
	//}
	isValid, taskType := util.GetTaskTypeFromFileName(taskId)

	if !isValid {
		glog.Errorf("file name %s is not valid", taskId)
		os.Exit(1)
	}

	// Only overwrite taskType if necessary
	if lcm.TaskType == "" {

		lcm.TaskType = taskType
		//lcm.TaskType = "DeltaDelete"
		os.Setenv(util.TaskTypeDeltaDelete, taskType) //for delete
	} else {
		// Ensure that the taskType from the file matches the existing lcm.TaskType
		if lcm.TaskType != taskType {
			glog.Warningf("Mismatched task types: expected %s, got %s", lcm.TaskType, taskType)
		}
	}

	if lcm.TaskType == util.TaskTypeFull {

		lcm.AzTaskResultMgr = dbmgr.GetTaskEventHubClient(os.Getenv(Full_EventHub_ConnStr), os.Getenv(Full_EventHub_Task))
		lcm.AzJobResultMgr = dbmgr.GetJobEventHubClient(os.Getenv(Full_EventHub_ConnStr), os.Getenv(Full_EventHub_Job))
	}

	if lcm.TaskType == util.TaskTypeDeltaUpsert {

		lcm.AzTaskResultMgr = dbmgr.GetTaskEventHubClient(os.Getenv(Delta_EventHub_ConnStr), os.Getenv(Delta_EventHub_UpsertTask))
		lcm.AzJobResultMgr = dbmgr.GetJobEventHubClient(os.Getenv(Delta_EventHub_ConnStr), os.Getenv(Delta_EventHub_UpsertJob))
	}

	if lcm.TaskType == util.TaskTypeDeltaDelete {
		lcm.AzTaskResultMgr = dbmgr.GetTaskEventHubClient(os.Getenv(Delta_EventHub_ConnStr), os.Getenv(Delta_EventHub_DeleteTask))
		lcm.AzJobResultMgr = dbmgr.GetJobEventHubClient(os.Getenv(Delta_EventHub_ConnStr), os.Getenv(Delta_EventHub_DeleteJob))
	}

	// fileName := taskId + ".json"
	// jsonFile, err := os.Open(fileName)
	// if err != nil {
	// 	panic(err)
	// }
	// defer jsonFile.Close()
	// byteValue, err := ioutil.ReadAll(jsonFile)
	// if err != nil {
	// 	panic(err)
	// }

	// newJob := make([]dbmgr.JobData, 0, Task_Max_File)
	// err = json.Unmarshal(byteValue, &newJob)
	// if err != nil {
	// 	glog.Error("Error During Get Job Data From Json file %s", err)
	// }

	//for performance tuning
	// Open the JSON file
	jsonFile, err := os.Open(taskId + ".json")
	if err != nil {
		panic(err)
	}
	defer jsonFile.Close()
	// Create a JSON Decoder
	decoder := json.NewDecoder(jsonFile)
	// Read opening bracket for the array of JSON objects
	_, err = decoder.Token()
	if err != nil {
		glog.Error("Error reading opening bracket: ", err)
		panic(err)
	}
	// Initialize the slice with capacity
	newJob := make([]dbmgr.JobDataB2B, 0, 1000)
	// Decode each JSON object
	for decoder.More() {
		var job dbmgr.JobDataB2B
		err := decoder.Decode(&job)
		if err != nil {
			glog.Error("Error decoding JSON: ", err)
			panic(err)
		}
		newJob = append(newJob, job)
	}
	// Check for the closing bracket of the array
	_, err = decoder.Token()
	if err != nil {
		glog.Error("Error reading closing bracket: ", err)
		panic(err)
	}

	//620 err = json.Unmarshal([]byte(jobs), &newJob)
	//if err != nil {
	//	glog.Error("Error During Get Job Data From Redis %s", err)
	//}
	// bkName = os.Getenv("SOURCE_BUCKET")
	// //bkRegion = todo
	// connStr := os.Getenv("AZ_BLOB_CONN_STRING")
	// if len(connStr) == 0 {
	// 	glog.Error("blob connection string is empty")
	// }
	//appInfo := util.ParseAzBlobConnString(connStr)
	appInfoDest := make(map[string]string)
	destSaName := os.Getenv("AZ_BLOB_STORAGE_ACCOUNT")
	appInfoDest["AccountName"] = destSaName
	appInfoDest["ContainerName"] = os.Getenv("AZ_BLOB_CONTAINER")
	appInfoDest["AccountURL"] = fmt.Sprintf("https://%s.blob.core.windows.net", destSaName)

	lcm.AzBlobContainer = appInfoDest["ContainerName"]

	blobClient := azblobmgr.GetAzBlobContainerClientDest(appInfoDest)
	lcm.AzBlobMgrDest = blobClient
	//blobClient.GenerateSAS(appInfo)
	//blobClient.GetOrCreateSAS(appInfo)
	//b2b destUrl := blobClient.URLBuilderNoSAS(azBlobContainer, sourceBCName)
	//destUrl := blobClient.URLBuilderNoSAS(azBlobContainer, appInfoDest["AccountName"])
	lcm.AzBlobURLPrefix = blobClient.URLPrefixBuilder(azBlobContainer, sourceSaName)

	// lcm.AzBlobMgr = blobClient
	// blobClient.GenerateSAS(appInfo)
	// destUrl := blobClient.URLBuilder(azBlobContainer, bkName)

	// sourceUrlPresfix := fmt.Sprintf("https://%s.s3.%s.amazonaws.com", bkName, bkRegion)
	// lcm.SourceURLPrefix = sourceUrlPresfix + "/"
	// lcm.AzBlobURLPrefix = blobClient.URLPrefixBuilder(azBlobContainer, bkName)

	wholeMap := make(map[string]dbmgr.JobDataB2B, len(newJob))
	resultMap := make(map[string]dbmgr.JobDataB2B, len(newJob))
	lcm.SkippedDelNum = 0

	var (
		sourceSAccountName, containerName string
		err1                              error
	)

	for _, jobItem := range newJob {
		trimStringFieldsB2B(&jobItem)

		glog.Debugf("Processing Job: %v", jobItem)

		toTier = jobItem.DestTier

		sourceUrl = jobItem.SourceUrl
		// containerURL, err := util.ParseContainerURL(sourceUrl)
		// if err != nil {
		// 	glog.Fatalf("Failed to parse container URL: %v", err)
		// }
		//glog.Debugf("Container URL: %s", containerURL)

		//sourceUrlPresfix = containerURL

		// re := regexp.MustCompile(`https://(\w+).blob.core.windows.net/(\w+)/(.*)`)
		// matches := re.FindStringSubmatch(sourceUrl)
		// if len(matches) != 0 {
		// 	sourceSaName = matches[1]
		// 	containerName = matches[2]

		var once sync.Once
		once.Do(func() {
			// Check if the Source URL is for GCP
			u, errParse := url.Parse(sourceUrl)
			isGCP := false
			if errParse == nil && u.Host != "" && (strings.Contains(u.Host, "storage.cloud.google.com") || strings.Contains(u.Host, "storage.googleapis.com")) {
				isGCP = true
			}

			if isGCP {
				parts, err := NewGCPURLParts(*u)
				if err != nil {
					glog.Errorf("Failed to parse GCP URL: %v", err)
				}
				// For GCP, we treat the bucket as the container
				containerName = parts.BucketName
				// Base GCP URL
				sourceUrlPresfix = fmt.Sprintf("https://%s", u.Host)

				lcm.AzBlobContainer = containerName

				// Construct the source prefix: https://storage.cloud.google.com/bucket/path/
				lcm.SourceURLPrefix = fmt.Sprintf("%s/%s/", sourceUrlPresfix, containerName)
				lcm.SourceURLPrefix = lcm.SourceURLPrefix + jobItem.FilePath

				// Update destination prefix
				lcm.AzBlobURLPrefix = lcm.AzBlobURLPrefix + jobItem.DestFilePath

			} else {
				// Assume Azure Blob Storage
				if sourceSAccountName, containerName, err1 = util.ParseSourceURL(sourceUrl); err1 != nil {
					glog.Errorf("Failed to parse source URL: %v", err1)
				}

				//if len(sourceUrlPresfix) == 0 {
				sourceUrlPresfix = fmt.Sprintf("https://%s.blob.core.windows.net", sourceSAccountName)
				//}
				if lcm.AzBlobMgrSrc == nil {
					appInfoSrc := map[string]string{
						"AccountName":   sourceSAccountName,
						"ContainerName": containerName,
						"AccountURL":    sourceUrlPresfix,
					}

					lcm.AzBlobContainer = appInfoSrc["ContainerName"]

					lcm.AzBlobMgrSrc = azblobmgr.GetAzBlobContainerClientSrc(appInfoSrc)
					//sourceUrlPresfix = fmt.Sprintf("%s/%s", sourceUrlPresfix, containerName)
					lcm.SourceURLPrefix = fmt.Sprintf("%s/%s/", sourceUrlPresfix, containerName)

					lcm.SourceURLPrefix = lcm.SourceURLPrefix + jobItem.FilePath

					lcm.AzBlobURLPrefix = lcm.AzBlobURLPrefix + jobItem.DestFilePath

				}
			}
		})

		lcm.AzBlobContainer = fmt.Sprintf("%s/%s", containerName, jobItem.FilePath)

		jobItem.ContainerName = containerName

		if lcm.TaskType == util.TaskTypeDeltaDelete {

			//lcm.AzBlobContainer = appInfoDest["ContainerName"]

			jobItem.ContainerName = appInfoDest["ContainerName"]

			// once.Do(func() {
			// 	lcm.AzBlobContainer = appInfoDest["ContainerName"]
			// })

			lcm.AzBlobContainer = fmt.Sprintf("%s/%s", appInfoDest["ContainerName"], jobItem.DestFilePath)

			//isExistObj, err := checkS3FileExist(bkName, jobItem.FilePath, jobItem.Region, endpoint)
			isExistObj, err := isSingleBlobForDelSrc(fmt.Sprintf("%s%s", jobItem.FilePath, jobItem.FileName))

			if isExistObj { //Azure blob exists
				//if checkS3FileExistWithRetry(bkName, jobItem.FilePath, jobItem.Region) {

				wholeMap[fmt.Sprintf("%s/%s%s", jobItem.ContainerName, jobItem.DestFilePath, jobItem.FileName)] = jobItem

				lcm.SkippedDelNum = lcm.SkippedDelNum + 1

				lcm.SubmitJobLogBeforeCmdB2B(jobItem, Job_Delete_Message_SkippedSuccess, Job_Status_SkippedSuccess)

			} else if err == nil || err.Error() == Error_Authorization_Failure {

				glog.Debugf("File %s is not existing in Azure Blob.", jobItem.SourceUrl)

				wholeMap[fmt.Sprintf("%s/%s%s", jobItem.ContainerName, jobItem.DestFilePath, jobItem.FileName)] = jobItem

				isValidBlob, err := isSingleBlobForDelDest(fmt.Sprintf("%s%s", jobItem.DestFilePath, jobItem.FileName))
				if isValidBlob {

					resultMap[fmt.Sprintf("%s/%s%s", jobItem.ContainerName, jobItem.DestFilePath, jobItem.FileName)] = jobItem
				} else if err != nil {

					lcm.SubmitJobLogBeforeCmdB2B(jobItem, err.Error(), Job_Status_Failed)
				} else {

					lcm.SubmitJobLogBeforeCmdB2B(jobItem, Job_Delete_Message_SkippedFail, Job_Status_Failed)
				}
			} else {

				lcm.SubmitJobLogBeforeCmdB2B(jobItem, err.Error(), Job_Status_Failed)
			}

		} else {

			//handleBlobArchiving(jobItem, wholeMap, resultMap, lcm)
			handleBlobArchivingGCS(jobItem, wholeMap, resultMap, lcm)

		}

	}

	// if len(wholeMap) == lcm.NonDeleteNum {

	// 	//lcm.SubmitTaskResultToeEventHub(lcm.NonDeleteNum, lcm.NonDeleteNum, lcm.NonDeleteNum, 0, 0)
	// 	lcm.AzTaskResultMgr.AzTaskResultData.NumberOfFiles = lcm.NonDeleteNum
	// 	lcm.AzTaskResultMgr.SetAzTaskResult(lcm.TaskIDFromParam, lcm.TaskType, true, lcm.NonDeleteNum, lcm.NonDeleteNum, 0, 0, util.CalFileSize(wholeMap))
	// }

	lcm.FileSliceB2B = wholeMap

	listFile := createTempFile(resultMap, lcm.TaskIDFromParam)

	// connStr := os.Getenv("AZ_BLOB_CONN_STRING")
	// if len(connStr) == 0 {
	// 	glog.Error("blob connection string is empty")
	// }
	// appInfo := util.ParseAzBlobConnString(connStr)
	// blobClient := azblobmgr.GetAzBlobContainerClient(appInfo)
	// Set Az Queue Client if ENV ADD_TO_QUEUE is true
	// if ok, _ := strconv.ParseBool(os.Getenv("ADD_TO_QUEUE")); ok {
	// 	blobClient.SetAzQueueClient(appInfo)
	// }

	// lcm.AzBlobMgr = blobClient
	// blobClient.GenerateSAS(appInfo)
	// destUrl := blobClient.URLBuilder(azBlobContainer, bkName)
	for index, item := range os.Args {
		//630
		if index == 1 && item == Copy {
			result = append(result, Copy)
			continue
		}
		if index == 1 && item == Remove {
			result = append(result, Remove)
			continue
		}
		switch item {
		case "-t", "-j", taskId, jobId:
			continue
		default:
			result = append(result, item)
		}

	}

	//sourceUrlPresfix := fmt.Sprintf("https://%s.s3.%s.amazonaws.com", bkName, bkRegion)
	//sourceUrlPresfix := fmt.Sprintf("https://%s.oss-%s.aliyuncs.com", bkName, bkRegion)
	//lcm.SourceURLPrefix = fmt.Sprintf("%s/", sourceUrlPresfix)
	// lcm.AzBlobURLPrefix = blobClient.URLPrefixBuilder(azBlobContainer, bkName)

	if lcm.TaskType == util.TaskTypeDeltaDelete {
		// lcm.AzTaskResultMgr = dbmgr.GetTaskEventHubClient(os.Getenv(Delta_EventHub_ConnStr), os.Getenv(Delta_EventHub_DeleteTask))
		// lcm.AzJobResultMgr = dbmgr.GetJobEventHubClient(os.Getenv(Delta_EventHub_ConnStr), os.Getenv(Delta_EventHub_DeleteJob))
		result = append(result, lcm.AzBlobURLPrefix, "--recursive=true",
			"--list-of-files",
			listFile)
	} else {
		result = append(result, lcm.SourceURLPrefix, lcm.AzBlobURLPrefix, "--recursive=true")

		if toTier != "" {
			result = append(result, "--block-blob-tier="+toTier) //"--s2s-preserve-access-tier=false", //"--block-blob-tier=Archive",
		}

		result = append(result, "--list-of-files",
			listFile)
	}

	return result
	// []string{
	// 	"azcopy",
	// 	"copy",
	// 	fmt.Sprintf("https://%s.s3.%s.amazonaws.com", bkName, bkRegion),
	// 	destUrl,
	// 	"--list-of-files",
	// 	listFile,
	// }
}

func handleBlobArchiving(jobItem dbmgr.JobDataB2B, wholeMap, resultMap map[string]dbmgr.JobDataB2B, lcm *lifecycleMgr) {
	// Skip archive checking for GCP sources - GCP does not need archive status checks
	if strings.Contains(lcm.SourceURLPrefix, "storage.cloud.google.com") || strings.Contains(lcm.SourceURLPrefix, "storage.googleapis.com") {
		// For GCP sources, directly add to both maps without archive checks
		resultMap[fmt.Sprintf("%s/%s%s", jobItem.ContainerName, jobItem.FilePath, jobItem.FileName)] = jobItem
		wholeMap[fmt.Sprintf("%s/%s%s", jobItem.ContainerName, jobItem.FilePath, jobItem.FileName)] = jobItem
		return
	}

	/*
		// Check if the source blob is archived
		isArchivedSrc, errSrc := isArchivedSingleBlobSrc(fmt.Sprintf("%s%s", jobItem.FilePath, jobItem.FileName))

		// Check if the destination blob is archived
		isArchivedDest, errDest := isArchivedSingleBlobDest(fmt.Sprintf("%s%s", jobItem.DestFilePath, jobItem.FileName))

		// Check if either the source or destination blob is archived
		if isArchivedSrc || isArchivedDest {
			lcm.SkippedDelNum++
			// If either blob is archived, add it to wholeMap and submit job log
			wholeMap[fmt.Sprintf("%s/%s%s", jobItem.ContainerName, jobItem.FilePath, jobItem.FileName)] = jobItem
			lcm.SubmitJobLogBeforeCmdB2B(jobItem, Job_Archived_Message_SkippedSuccess, Job_Status_SkippedSuccess)
			//return
		} else if (errDest != nil && strings.Contains(errDest.Error(), Error_BlobNotFound) && errSrc == nil) || (errSrc == nil && errDest == nil) {
			// If neither blob is archived and no errors occurred, add to both resultMap and wholeMap
			resultMap[fmt.Sprintf("%s/%s%s", jobItem.ContainerName, jobItem.FilePath, jobItem.FileName)] = jobItem
			wholeMap[fmt.Sprintf("%s/%s%s", jobItem.ContainerName, jobItem.FilePath, jobItem.FileName)] = jobItem
		} else if errSrc != nil && strings.Contains(errSrc.Error(), Error_BlobNotFound) {

			lcm.SkippedDelNum++
			wholeMap[fmt.Sprintf("%s/%s%s", jobItem.ContainerName, jobItem.FilePath, jobItem.FileName)] = jobItem
			lcm.SubmitJobLogBeforeCmdB2B(jobItem, Job_BlobNotFound_Message_SkippedSuccess, Job_Status_SkippedSuccess)

		} else {

			wholeMap[fmt.Sprintf("%s/%s%s", jobItem.ContainerName, jobItem.FilePath, jobItem.FileName)] = jobItem
			lcm.SubmitJobLogBeforeCmdB2B(jobItem, "Checking archived blob error occurred", Job_Status_Failed)

		}
	*/

}

func handleBlobArchivingGCS(jobItem dbmgr.JobDataB2B, wholeMap, resultMap map[string]dbmgr.JobDataB2B, lcm *lifecycleMgr) {
	// GCP sources do not have Archive tier; only validate destination Azure blob tier.
	isArchivedDest, errDest := isArchivedSingleBlobDest(fmt.Sprintf("%s%s", jobItem.DestFilePath, jobItem.FileName))

	if isArchivedDest {
		lcm.SkippedDelNum++
		wholeMap[fmt.Sprintf("%s/%s%s", jobItem.ContainerName, jobItem.FilePath, jobItem.FileName)] = jobItem
		lcm.SubmitJobLogBeforeCmdB2B(jobItem, Job_Archived_Message_SkippedSuccess, Job_Status_SkippedSuccess)
		return
	}

	if errDest == nil || (errDest != nil && strings.Contains(errDest.Error(), Error_BlobNotFound)) {
		resultMap[fmt.Sprintf("%s/%s%s", jobItem.ContainerName, jobItem.FilePath, jobItem.FileName)] = jobItem
		wholeMap[fmt.Sprintf("%s/%s%s", jobItem.ContainerName, jobItem.FilePath, jobItem.FileName)] = jobItem
		return
	}

	wholeMap[fmt.Sprintf("%s/%s%s", jobItem.ContainerName, jobItem.FilePath, jobItem.FileName)] = jobItem
	lcm.SubmitJobLogBeforeCmdB2B(jobItem, "Checking archived destination blob error occurred", Job_Status_Failed)
}

func (lcm *lifecycleMgr) getTaskType() string {

	return lcm.TaskType
}

// LoadConfigurationFileFromYaml loads config.yaml file into config.Config struct
// The DEBUG_MODE now read from system Env.
func (lcm *lifecycleMgr) LoadConfigurationFileFromYaml(taskID string, jobID string) {
	//Loading Config from config.yaml
	// goS3Config, err := config.LoadYaml()
	// if err != nil {
	// 	panic("Please Check config.yaml")
	// }
	lcm.TaskIDFromParam = taskID
	lcm.JobIDFromParam = jobID
	debugMode, _ := strconv.Atoi(os.Getenv("DEBUG_MODE"))
	lcm.DebugMode = debugMode
	// goS3Config.DEBUG_MODE = debugMode
	// lcm.YamlConfig = goS3Config
}

// getAzureKeyFromRedisOrKeyVault get value with key from Redis.
// If the key is not exist in Redis, read from Key Vault and cache it into Redis
func getAzureKeyFromRedisOrKeyVault(key string) string {
	val := dbmgr.GetRedisKey(key, lcm.JobDataRedisClient)
	if len(val) == 0 {
		val = getKeyVaultKey(key)
		if len(val) != 0 {
			err := lcm.JobDataRedisClient.Set(context.Background(), key, val, 0).Err()
			if err != nil {
				fmt.Printf("can't cache key %s to redis", key)
			}
		}

	}
	return val
}

// // GetAzureKeys get keys (bucket name, storage credential, aws credential, cosmos db credential, and cosmos db partition key range for MigrationTasks)
// func (lcm *lifecycleMgr) GetAzureKeys(jobId string) {

// 	//Decrypt redis cache with AES 128
// 	redisHost, redisPWD, redisSSL, err := util.GetAzureRedisCredential("RedisCache", jobId)
// 	if err != nil {
// 		lcm.GetKeyVault()
// 	}

// 	// if len(redisHost) != 0 {
// 	// 	lcm.YamlConfig.REDIS_HOST = redisHost
// 	// 	lcm.YamlConfig.REDIS_PSW = redisPWD
// 	// }

// 	if len(redisHost) != 0 {
// 		os.Setenv(Redis_Host, redisHost)
// 		os.Setenv(Redis_PWD, redisPWD)
// 	}
// 	//TODO exit if redisHost is empty

// 	bucketName := os.Getenv("SOURCE_BUCKET")
// 	storageAccount := os.Getenv("AZ_BLOB_STORAGE_ACCOUNT")
// 	awsKeyId := fmt.Sprintf("%s-%s", bucketName, "AWSACCESSKEYID")
// 	awsSecretKeyId := fmt.Sprintf("%s-%s", bucketName, "AWSSECRETACCESSKEY")

// 	//Set Redis client (with or without ssl)
// 	if lcm.JobDataRedisClient == nil {
// 		if ok, _ := strconv.ParseBool(redisSSL); ok {
// 			lcm.JobDataRedisClient = goredis.NewClient(&goredis.Options{
// 				Addr:      os.Getenv(Redis_Host),
// 				Password:  os.Getenv(Redis_PWD),
// 				DB:        0,
// 				TLSConfig: &tls.Config{MinVersion: tls.VersionTLS12},
// 			})
// 		} else {
// 			lcm.JobDataRedisClient = goredis.NewClient(&goredis.Options{
// 				Addr:     os.Getenv(Redis_Host),
// 				Password: os.Getenv(Redis_PWD),
// 				DB:       0,
// 			})
// 		}
// 	}

// 	//Get Env from Redis or Key Vault. If the value is not exist in Redis, read from Key Vault and cache into Redis
// 	//620 pkRange, _ := lcm.JobDataRedisClient.Get(context.TODO(), "COSMOS-PK-RANGE").Result()

// 	storageConn := getAzureKeyFromRedisOrKeyVault(storageAccount)
// 	aws_access_key_id := getAzureKeyFromRedisOrKeyVault(awsKeyId)
// 	//620 cosmosdbConn := getAzureKeyFromRedisOrKeyVault("CosmosDb--ConnectionString", lcm.JobDataRedisClient)
// 	aws_secret_access_key := getAzureKeyFromRedisOrKeyVault(awsSecretKeyId)
// 	//620
// 	eventhubconnStrFull := getAzureKeyFromRedisOrKeyVault("EventHubNameSpace--Full--FullyQualifiedNamespace")
// 	eventhubNameFullTask := getAzureKeyFromRedisOrKeyVault("EventHubNameSpace--Full--EventHubName--Task")
// 	eventhubNameFullJob := getAzureKeyFromRedisOrKeyVault("EventHubNameSpace--Full--EventHubName--Job")

// 	eventhubconnStrDelta := getAzureKeyFromRedisOrKeyVault("EventHubNameSpace--Delta--FullyQualifiedNamespace")
// 	eventhubNameUpsertTask := getAzureKeyFromRedisOrKeyVault("EventHubNameSpace--Delta--EventHubName--UpsertTask")
// 	eventhubNameUpsertJob := getAzureKeyFromRedisOrKeyVault("EventHubNameSpace--Delta--EventHubName--UpsertJob")

// 	// eventhubNameDeleteTask := getAzureKeyFromRedisOrKeyVault("EventHubNameSpace--Delta--EventHubName--DeleteJob")
// 	// eventhubNameDeleteJob := getAzureKeyFromRedisOrKeyVault("EventHubNameSpace--Delta--EventHubName--DeleteTask")

// 	lcm.AzTaskResultMgr = dbmgr.GetTaskEventHubClient(eventhubconnStrFull, eventhubNameFullTask)
// 	lcm.AzJobResultMgr = dbmgr.GetJobEventHubClient(eventhubconnStrFull, eventhubNameFullJob)

// 	//Exit if any of these Envs are not exist.
// 	//if len(storageConn) == 0 || len(aws_access_key_id) == 0 || len(aws_secret_access_key) == 0 || len(cosmosdbConn) == 0 {
// 	if len(storageConn) == 0 || len(aws_access_key_id) == 0 || len(aws_secret_access_key) == 0 || len(eventhubconnStrFull) == 0 {

// 		glog.Error("Cannot get keys (pkrange, storage conn, aws access key id, eventhub conn, aws secret access key) from key vault or redis")
// 		os.Exit(-1)
// 	}

// 	//Set Default value of the random range of Cosmos DB (MigrationTasks Container) partition key
// 	//Default range is [0,99999)
// 	// if len(pkRange) != 0 {
// 	// 	lcm.CosmosPKRange, err = strconv.Atoi(pkRange)
// 	// 	if err != nil {
// 	// 		glog.Errorf("Error during convert pkrange %s to int : %s", pkRange, err)
// 	// 	}
// 	// } else {
// 	// 	pkRange = getKeyVaultKey("COSMOS-PK-RANGE")
// 	// 	if len(pkRange) != 0 {
// 	// 		lcm.CosmosPKRange, err = strconv.Atoi(pkRange)
// 	// 		if err != nil {
// 	// 			glog.Errorf("Error during convert pkrange %s to int : %s", pkRange, err)
// 	// 		}
// 	// 		err := lcm.JobDataRedisClient.Set(context.Background(), "COSMOS-PK-RANGE", pkRange, 0).Err()
// 	// 		if err != nil {
// 	// 			glog.Errorf("can't cache key %s to redis", "COSMOS-PK-RANGE")
// 	// 		}
// 	// 	} else {
// 	// 		lcm.CosmosPKRange = 100000
// 	// 	}
// 	// }

// 	//Parsing Cosmos DB connection string
// 	//620 cosmosHost, cosmosPWD, err := util.ParseAzureCosmosConn(cosmosdbConn)
// 	// if err != nil {
// 	// 	os.Exit(-1)
// 	// }
// 	// if len(cosmosHost) != 0 {
// 	// 	lcm.YamlConfig.COSMOS_ENDPOINT = cosmosHost
// 	// 	lcm.YamlConfig.COSMOS_KEY = cosmosPWD
// 	// 	cosmosDBName := getAzureKeyFromRedisOrKeyVault("CosmosDb--Database", lcm.JobDataRedisClient)
// 	// 	if len(cosmosDBName) != 0 {
// 	// 		lcm.YamlConfig.COSMOS_DB = cosmosDBName
// 	// 	}
// 	// }

// 	//TODO Why this ?
// 	// if len(redisHost) != 0 {
// 	// 	lcm.YamlConfig.REDIS_HOST = redisHost
// 	// 	lcm.YamlConfig.REDIS_PSW = redisPWD
// 	// }

// 	//Add Envs into system Env Value (Project Global Var)
// 	os.Setenv("AZCOPY_PARALLEL_STAT_FILES", "true")
// 	os.Setenv("AWS_ACCESS_KEY_ID", aws_access_key_id)
// 	os.Setenv("AWS_SECRET_ACCESS_KEY", aws_secret_access_key)
// 	os.Setenv("AZ_BLOB_CONN_STRING", storageConn)
// 	//630
// 	os.Setenv(Delta_EventHub_ConnStr, eventhubconnStrDelta)
// 	os.Setenv(Delta_EventHub_UpsertTask, eventhubNameUpsertTask)
// 	os.Setenv(Delta_EventHub_UpsertJob, eventhubNameUpsertJob)

// }
func (lcm *lifecycleMgr) GetJobDataFromjsonfileGCS(taskId string, jobId string) []string {
	var sourceSaName, sourceUrlPresfix, sourceUrl, toTier string
	//sourceBCName = os.Getenv("SOURCE_BUCKET")
	azBlobContainer := os.Getenv("AZ_BLOB_CONTAINER")
	result := []string{}
	//620 rdb := lcm.JobDataRedisClient

	//THIS IS NO LONGER NEEDED
	//if lcm.JobDataRedisClient == nil {
	//	rdb = goredis.NewClient(&goredis.Options{
	//		Addr:      lcm.YamlConfig.REDIS_HOST,
	//		Password:  lcm.YamlConfig.REDIS_PSW,
	//		DB:        0,
	//		TLSConfig: &tls.Config{MinVersion: tls.VersionTLS12},
	//	})
	//	lcm.JobDataRedisClient = rdb
	//}

	//620 newJob := []dbmgr.JobData{}

	//620 jobs, err := rdb.Get(context.TODO(), lcm.TaskIDFromParam).Result()
	//
	//if err != nil {
	//	glog.Errorf("Error During Get Job Data From Redis %s", err)
	//	os.Exit(-1)
	//}
	isValid, taskType := util.GetTaskTypeFromFileName(taskId)

	if !isValid {
		glog.Errorf("file name %s is not valid", taskId)
		os.Exit(1)
	}

	// Only overwrite taskType if necessary
	if lcm.TaskType == "" {

		lcm.TaskType = taskType
		//lcm.TaskType = "DeltaDelete"
		os.Setenv(util.TaskTypeDeltaDelete, taskType) //for delete
	} else {
		// Ensure that the taskType from the file matches the existing lcm.TaskType
		if lcm.TaskType != taskType {
			glog.Warningf("Mismatched task types: expected %s, got %s", lcm.TaskType, taskType)
		}
	}

	if lcm.TaskType == util.TaskTypeFull {

		lcm.AzTaskResultMgr = dbmgr.GetTaskEventHubClient(os.Getenv(Full_EventHub_ConnStr), os.Getenv(Full_EventHub_Task))
		lcm.AzJobResultMgr = dbmgr.GetJobEventHubClient(os.Getenv(Full_EventHub_ConnStr), os.Getenv(Full_EventHub_Job))
	}

	if lcm.TaskType == util.TaskTypeDeltaUpsert {

		lcm.AzTaskResultMgr = dbmgr.GetTaskEventHubClient(os.Getenv(Delta_EventHub_ConnStr), os.Getenv(Delta_EventHub_UpsertTask))
		lcm.AzJobResultMgr = dbmgr.GetJobEventHubClient(os.Getenv(Delta_EventHub_ConnStr), os.Getenv(Delta_EventHub_UpsertJob))
	}

	if lcm.TaskType == util.TaskTypeDeltaDelete {
		lcm.AzTaskResultMgr = dbmgr.GetTaskEventHubClient(os.Getenv(Delta_EventHub_ConnStr), os.Getenv(Delta_EventHub_DeleteTask))
		lcm.AzJobResultMgr = dbmgr.GetJobEventHubClient(os.Getenv(Delta_EventHub_ConnStr), os.Getenv(Delta_EventHub_DeleteJob))
	}

	// fileName := taskId + ".json"
	// jsonFile, err := os.Open(fileName)
	// if err != nil {
	// 	panic(err)
	// }
	// defer jsonFile.Close()
	// byteValue, err := ioutil.ReadAll(jsonFile)
	// if err != nil {
	// 	panic(err)
	// }

	// newJob := make([]dbmgr.JobData, 0, Task_Max_File)
	// err = json.Unmarshal(byteValue, &newJob)
	// if err != nil {
	// 	glog.Error("Error During Get Job Data From Json file %s", err)
	// }

	//for performance tuning
	// Open the JSON file
	jsonFile, err := os.Open(taskId + ".json")
	if err != nil {
		panic(err)
	}
	defer jsonFile.Close()
	// Create a JSON Decoder
	decoder := json.NewDecoder(jsonFile)
	// Read opening bracket for the array of JSON objects
	_, err = decoder.Token()
	if err != nil {
		glog.Error("Error reading opening bracket: ", err)
		panic(err)
	}
	// Initialize the slice with capacity
	newJob := make([]dbmgr.JobDataB2B, 0, 1000)
	// Decode each JSON object
	for decoder.More() {
		var job dbmgr.JobDataB2B
		err := decoder.Decode(&job)
		if err != nil {
			glog.Error("Error decoding JSON: ", err)
			panic(err)
		}
		newJob = append(newJob, job)
	}
	// Check for the closing bracket of the array
	_, err = decoder.Token()
	if err != nil {
		glog.Error("Error reading closing bracket: ", err)
		panic(err)
	}

	//620 err = json.Unmarshal([]byte(jobs), &newJob)
	//if err != nil {
	//	glog.Error("Error During Get Job Data From Redis %s", err)
	//}
	// bkName = os.Getenv("SOURCE_BUCKET")
	// //bkRegion = todo
	// connStr := os.Getenv("AZ_BLOB_CONN_STRING")
	// if len(connStr) == 0 {
	// 	glog.Error("blob connection string is empty")
	// }
	//appInfo := util.ParseAzBlobConnString(connStr)
	appInfoDest := make(map[string]string)
	destSaName := os.Getenv("AZ_BLOB_STORAGE_ACCOUNT")
	appInfoDest["AccountName"] = destSaName
	appInfoDest["ContainerName"] = os.Getenv("AZ_BLOB_CONTAINER")
	appInfoDest["AccountURL"] = fmt.Sprintf("https://%s.blob.core.windows.net", destSaName)

	lcm.AzBlobContainer = appInfoDest["ContainerName"]

	blobClient := azblobmgr.GetAzBlobContainerClientDest(appInfoDest)
	lcm.AzBlobMgrDest = blobClient
	//blobClient.GenerateSAS(appInfo)
	//blobClient.GetOrCreateSAS(appInfo)
	//b2b destUrl := blobClient.URLBuilderNoSAS(azBlobContainer, sourceBCName)
	//destUrl := blobClient.URLBuilderNoSAS(azBlobContainer, appInfoDest["AccountName"])
	lcm.AzBlobURLPrefix = blobClient.URLPrefixBuilder(azBlobContainer, sourceSaName)

	// lcm.AzBlobMgr = blobClient
	// blobClient.GenerateSAS(appInfo)
	// destUrl := blobClient.URLBuilder(azBlobContainer, bkName)

	// sourceUrlPresfix := fmt.Sprintf("https://%s.s3.%s.amazonaws.com", bkName, bkRegion)
	// lcm.SourceURLPrefix = sourceUrlPresfix + "/"
	// lcm.AzBlobURLPrefix = blobClient.URLPrefixBuilder(azBlobContainer, bkName)

	wholeMap := make(map[string]dbmgr.JobDataB2B, len(newJob))
	resultMap := make(map[string]dbmgr.JobDataB2B, len(newJob))
	lcm.SkippedDelNum = 0

	var (
		sourceSAccountName, containerName string
		err1                              error
	)

	for _, jobItem := range newJob {
		trimStringFieldsB2B(&jobItem)

		glog.Debugf("Processing Job: %v", jobItem)

		toTier = jobItem.DestTier

		sourceUrl = jobItem.SourceUrl
		// containerURL, err := util.ParseContainerURL(sourceUrl)
		// if err != nil {
		// 	glog.Fatalf("Failed to parse container URL: %v", err)
		// }
		//glog.Debugf("Container URL: %s", containerURL)

		//sourceUrlPresfix = containerURL

		// re := regexp.MustCompile(`https://(\w+).blob.core.windows.net/(\w+)/(.*)`)
		// matches := re.FindStringSubmatch(sourceUrl)
		// if len(matches) != 0 {
		// 	sourceSaName = matches[1]
		// 	containerName = matches[2]

		var once sync.Once
		once.Do(func() {
			// Check if the Source URL is for GCP
			u, errParse := url.Parse(sourceUrl)
			isGCP := false
			if errParse == nil && u.Host != "" && (strings.Contains(u.Host, "storage.cloud.google.com") || strings.Contains(u.Host, "storage.googleapis.com")) {
				isGCP = true
			}

			if isGCP {
				parts, err := NewGCPURLParts(*u)
				if err != nil {
					glog.Errorf("Failed to parse GCP URL: %v", err)
				}
				// For GCP, we treat the bucket as the container
				containerName = parts.BucketName
				// Base GCP URL
				sourceUrlPresfix = fmt.Sprintf("https://%s", u.Host)

				lcm.AzBlobContainer = containerName

				// Construct the source prefix: https://storage.cloud.google.com/bucket/path/
				lcm.SourceURLPrefix = fmt.Sprintf("%s/%s/", sourceUrlPresfix, containerName)
				lcm.SourceURLPrefix = lcm.SourceURLPrefix + jobItem.FilePath

				// Update destination prefix
				lcm.AzBlobURLPrefix = lcm.AzBlobURLPrefix + jobItem.DestFilePath

			} else {
				// Assume Azure Blob Storage
				if sourceSAccountName, containerName, err1 = util.ParseSourceURL(sourceUrl); err1 != nil {
					glog.Errorf("Failed to parse source URL: %v", err1)
				}

				//if len(sourceUrlPresfix) == 0 {
				sourceUrlPresfix = fmt.Sprintf("https://%s.blob.core.windows.net", sourceSAccountName)
				//}
				if lcm.AzBlobMgrSrc == nil {
					appInfoSrc := map[string]string{
						"AccountName":   sourceSAccountName,
						"ContainerName": containerName,
						"AccountURL":    sourceUrlPresfix,
					}

					lcm.AzBlobContainer = appInfoSrc["ContainerName"]

					lcm.AzBlobMgrSrc = azblobmgr.GetAzBlobContainerClientSrc(appInfoSrc)
					//sourceUrlPresfix = fmt.Sprintf("%s/%s", sourceUrlPresfix, containerName)
					lcm.SourceURLPrefix = fmt.Sprintf("%s/%s/", sourceUrlPresfix, containerName)

					lcm.SourceURLPrefix = lcm.SourceURLPrefix + jobItem.FilePath

					lcm.AzBlobURLPrefix = lcm.AzBlobURLPrefix + jobItem.DestFilePath

				}
			}
		})

		lcm.AzBlobContainer = fmt.Sprintf("%s/%s", containerName, jobItem.FilePath)

		jobItem.ContainerName = containerName

		if lcm.TaskType == util.TaskTypeDeltaDelete {

			//lcm.AzBlobContainer = appInfoDest["ContainerName"]

			jobItem.ContainerName = appInfoDest["ContainerName"]

			// once.Do(func() {
			// 	lcm.AzBlobContainer = appInfoDest["ContainerName"]
			// })

			lcm.AzBlobContainer = fmt.Sprintf("%s/%s", appInfoDest["ContainerName"], jobItem.DestFilePath)

			//isExistObj, err := checkS3FileExist(bkName, jobItem.FilePath, jobItem.Region, endpoint)
			isExistObj, err := isSingleGCSObjectForDelSrc(fmt.Sprintf("%s%s", jobItem.FilePath, jobItem.FileName))

			if isExistObj { //Azure blob exists
				//if checkS3FileExistWithRetry(bkName, jobItem.FilePath, jobItem.Region) {

				wholeMap[fmt.Sprintf("%s/%s%s", jobItem.ContainerName, jobItem.DestFilePath, jobItem.FileName)] = jobItem

				lcm.SkippedDelNum = lcm.SkippedDelNum + 1

				lcm.SubmitJobLogBeforeCmdB2B(jobItem, Job_Delete_Message_SkippedSuccess, Job_Status_SkippedSuccess)

			} else if err == nil || err.Error() == Error_Authorization_Failure {

				glog.Debugf("File %s is not existing in Azure Blob.", jobItem.SourceUrl)

				wholeMap[fmt.Sprintf("%s/%s%s", jobItem.ContainerName, jobItem.DestFilePath, jobItem.FileName)] = jobItem

				isValidBlob, err := isSingleBlobForDelDest(fmt.Sprintf("%s%s", jobItem.DestFilePath, jobItem.FileName))
				if isValidBlob {

					resultMap[fmt.Sprintf("%s/%s%s", jobItem.ContainerName, jobItem.DestFilePath, jobItem.FileName)] = jobItem
				} else if err != nil {

					lcm.SubmitJobLogBeforeCmdB2B(jobItem, err.Error(), Job_Status_Failed)
				} else {

					lcm.SubmitJobLogBeforeCmdB2B(jobItem, Job_Delete_Message_SkippedFail, Job_Status_Failed)
				}
			} else {

				lcm.SubmitJobLogBeforeCmdB2B(jobItem, err.Error(), Job_Status_Failed)
			}

		} else {

			handleBlobArchiving(jobItem, wholeMap, resultMap, lcm)

		}

	}

	// if len(wholeMap) == lcm.NonDeleteNum {

	// 	//lcm.SubmitTaskResultToeEventHub(lcm.NonDeleteNum, lcm.NonDeleteNum, lcm.NonDeleteNum, 0, 0)
	// 	lcm.AzTaskResultMgr.AzTaskResultData.NumberOfFiles = lcm.NonDeleteNum
	// 	lcm.AzTaskResultMgr.SetAzTaskResult(lcm.TaskIDFromParam, lcm.TaskType, true, lcm.NonDeleteNum, lcm.NonDeleteNum, 0, 0, util.CalFileSize(wholeMap))
	// }

	lcm.FileSliceB2B = wholeMap

	listFile := createTempFile(resultMap, lcm.TaskIDFromParam)

	// connStr := os.Getenv("AZ_BLOB_CONN_STRING")
	// if len(connStr) == 0 {
	// 	glog.Error("blob connection string is empty")
	// }
	// appInfo := util.ParseAzBlobConnString(connStr)
	// blobClient := azblobmgr.GetAzBlobContainerClient(appInfo)
	// Set Az Queue Client if ENV ADD_TO_QUEUE is true
	// if ok, _ := strconv.ParseBool(os.Getenv("ADD_TO_QUEUE")); ok {
	// 	blobClient.SetAzQueueClient(appInfo)
	// }

	// lcm.AzBlobMgr = blobClient
	// blobClient.GenerateSAS(appInfo)
	// destUrl := blobClient.URLBuilder(azBlobContainer, bkName)
	for index, item := range os.Args {
		//630
		if index == 1 && item == Copy {
			result = append(result, Copy)
			continue
		}
		if index == 1 && item == Remove {
			result = append(result, Remove)
			continue
		}
		switch item {
		case "-t", "-j", taskId, jobId:
			continue
		default:
			result = append(result, item)
		}

	}

	//sourceUrlPresfix := fmt.Sprintf("https://%s.s3.%s.amazonaws.com", bkName, bkRegion)
	//sourceUrlPresfix := fmt.Sprintf("https://%s.oss-%s.aliyuncs.com", bkName, bkRegion)
	//lcm.SourceURLPrefix = fmt.Sprintf("%s/", sourceUrlPresfix)
	// lcm.AzBlobURLPrefix = blobClient.URLPrefixBuilder(azBlobContainer, bkName)

	if lcm.TaskType == util.TaskTypeDeltaDelete {
		// lcm.AzTaskResultMgr = dbmgr.GetTaskEventHubClient(os.Getenv(Delta_EventHub_ConnStr), os.Getenv(Delta_EventHub_DeleteTask))
		// lcm.AzJobResultMgr = dbmgr.GetJobEventHubClient(os.Getenv(Delta_EventHub_ConnStr), os.Getenv(Delta_EventHub_DeleteJob))
		result = append(result, lcm.AzBlobURLPrefix, "--recursive=true",
			"--list-of-files",
			listFile)
	} else {
		result = append(result, lcm.SourceURLPrefix, lcm.AzBlobURLPrefix, "--recursive=true")

		if toTier != "" {
			result = append(result, "--block-blob-tier="+toTier) //"--s2s-preserve-access-tier=false", //"--block-blob-tier=Archive",
		}

		result = append(result, "--list-of-files",
			listFile)
	}

	return result
	// []string{
	// 	"azcopy",
	// 	"copy",
	// 	fmt.Sprintf("https://%s.s3.%s.amazonaws.com", bkName, bkRegion),
	// 	destUrl,
	// 	"--list-of-files",
	// 	listFile,
	// }
}

// GetKeyVault get keys from Key Vault if Redis is not exist.
func (lcm *lifecycleMgr) GetKeyVault() {

	if lcm.KeyVaultClient == nil {
		keyVaultUrl := os.Getenv("KeyVaultURI")
		if len(keyVaultUrl) == 0 {
			glog.Error("can't get KeyVaultURI from env")
			os.Exit(-1)
			// keyVaultName := lcm.YamlConfig.AZ_KEYVAULT
			// //print(keyVaultName)
			// if len(keyVaultName) == 0 {
			// 	keyVaultName = os.Getenv("AZ_KEYVAULT")
			// }
			// keyVaultUrl = fmt.Sprintf("https://%s.vault.azure.net/", keyVaultName)
		}
		cred, err := azidentity.NewDefaultAzureCredential(nil)
		if err != nil {
			glog.Error("failed to obtain a credential:%s \n %s", keyVaultUrl, err)
			os.Exit(-1)
		}
		// create azkeys client
		client, err := azsecrets.NewClient(keyVaultUrl, cred, nil)
		if err != nil {
			glog.Error("azure key vault init client failed", err)
			os.Exit(-1)
		}
		lcm.KeyVaultClient = client
	}

	// bucketName := os.Getenv("SOURCE_BUCKET")
	// storageAccount := os.Getenv("AZ_BLOB_STORAGE_ACCOUNT")
	// awsKeyId := fmt.Sprintf("%s-%s", bucketName, "AWSACCESSKEYID")           //
	// awsSecretKeyId := fmt.Sprintf("%s-%s", bucketName, "AWSSECRETACCESSKEY") //
	//cosmosConn := "AZ-CosmosDB-CONNECTION-STRING"
	// create credential

	// storageConn := getKeyVaultKey(storageAccount)
	// aws_access_key_id := getKeyVaultKey(awsKeyId)
	// //cosmosdbConn := getKeyVaultKey("CosmosDb--ConnectionString")

	// redisConn := getKeyVaultKey("Redis--ConnectionString")
	// aws_secret_access_key := getKeyVaultKey(awsSecretKeyId)
	// //if len(storageConn) == 0 || len(aws_access_key_id) == 0 || len(aws_secret_access_key) == 0 || len(cosmosdbConn) == 0 || len(redisConn) == 0 {
	// if len(storageConn) == 0 || len(aws_access_key_id) == 0 || len(aws_secret_access_key) == 0 || len(eventhubConn) == 0 || len(redisConn) == 0 {

	// 	os.Exit(-1)
	// }
	// cosmosHost, cosmosPWD, err := util.ParseAzureCosmosConn(cosmosdbConn)
	// if err != nil {
	// 	glog.Errorf("Cannot parse Azure Cosmos connection string : %s", err)
	// 	os.Exit(-1)
	// }
	//630
	// redisHost, redisPWD, _, err := util.ParseAzureRedisConn(redisConn)
	// if err != nil {
	// 	glog.Errorf("Cannot parse Azure Redis connection string : %s", err)
	// 	os.Exit(-1)
	// }
	// if len(cosmosHost) != 0 {
	// 	lcm.YamlConfig.COSMOS_ENDPOINT = cosmosHost
	// 	lcm.YamlConfig.COSMOS_KEY = cosmosPWD
	// 	cosmoseDBName := getKeyVaultKey("CosmosDb--Database")
	// 	if len(cosmoseDBName) != 0 {
	// 		lcm.YamlConfig.COSMOS_DB = cosmoseDBName
	// 	}
	// }

	//620
	// if len(eventhubConn) != 0 {
	// 	lcm.YamlConfig.EVENTHUB_CONN_STR = eventhubConn
	// 	lcm.YamlConfig.EVENTHUB_NAME_TASK = eventhubNameTask
	// 	lcm.YamlConfig.EVENTHUB_NAME_JOB = eventhubNameJob
	// }

	// if len(redisHost) != 0 {
	// 	lcm.YamlConfig.REDIS_HOST = redisHost
	// 	lcm.YamlConfig.REDIS_PSW = redisPWD
	// }
	// os.Setenv("AZCOPY_PARALLEL_STAT_FILES", "true")
	// os.Setenv("AWS_ACCESS_KEY_ID", aws_access_key_id)
	// os.Setenv("AWS_SECRET_ACCESS_KEY", aws_secret_access_key)
	// os.Setenv("AZ_BLOB_CONN_STRING", storageConn)

}

// getKeyVaultKey get value from Key Vault with key
func getKeyVaultKey(key string) string {
	if lcm.KeyVaultClient == nil {
		keyVaultUrl := os.Getenv("KeyVaultURI")
		if len(keyVaultUrl) == 0 {
			glog.Error("can't get KeyVaultURI from env")
			os.Exit(-1)
			// keyVaultName := lcm.YamlConfig.AZ_KEYVAULT
			// //print(keyVaultName)
			// if len(keyVaultName) == 0 {
			// 	keyVaultName = os.Getenv("AZ_KEYVAULT")
			// }
			// keyVaultUrl = fmt.Sprintf("https://%s.vault.azure.net/", keyVaultName)
		}
		cred, err := azidentity.NewDefaultAzureCredential(nil)
		if err != nil {
			glog.Error("failed to obtain a credential:%s \n %s", keyVaultUrl, err)
			os.Exit(-1)
		}
		// create azkeys client
		client, err := azsecrets.NewClient(keyVaultUrl, cred, nil)
		if err != nil {
			glog.Errorf("azure key vault init client failed: %s", err)
			os.Exit(-1)
		}
		lcm.KeyVaultClient = client
	}
	//fmt.Print(lcm.KeyVaultClient)
	var keyVaultVal string = ""
	resp, err := lcm.KeyVaultClient.GetSecret(context.TODO(), key, "", nil)
	if err != nil {
		glog.Errorf("key %s is null: %s", key, err)
	}

	keyVaultVal = *resp.Value
	if len(keyVaultVal) == 0 {
		glog.Errorf("key %s is null", keyVaultVal)
		os.Exit(-1)
	}
	return keyVaultVal
}

func (lcm *lifecycleMgr) SetOverwriteIfNew(overwriteValue string) {
	if overwriteValue == "IfSourceNewer" {
		lcm.OverwriteIfNew = true
	}
	lcm.OverwriteIfNew = false
}

// GetJobDataFromRedis read task detail information from redis.
// Add detail information to map lcm.FileSlice.
// Add transfer file names into temp file (txt).
// Rewrite "-t -j" args into Azcopy args.
// Return rewrote Azcopy args.
func (lcm *lifecycleMgr) GetJobDataFromjsonfileS3(taskId string, jobId string) []string {
	var bkName, bkRegion, sourceUrlPresfix, endpoint string
	bkName = os.Getenv("SOURCE_BUCKET")
	azBlobContainer := os.Getenv("AZ_BLOB_CONTAINER")
	result := []string{}
	//620 rdb := lcm.JobDataRedisClient

	//THIS IS NO LONGER NEEDED
	//if lcm.JobDataRedisClient == nil {
	//	rdb = goredis.NewClient(&goredis.Options{
	//		Addr:      lcm.YamlConfig.REDIS_HOST,
	//		Password:  lcm.YamlConfig.REDIS_PSW,
	//		DB:        0,
	//		TLSConfig: &tls.Config{MinVersion: tls.VersionTLS12},
	//	})
	//	lcm.JobDataRedisClient = rdb
	//}

	//620 newJob := []dbmgr.JobData{}

	//620 jobs, err := rdb.Get(context.TODO(), lcm.TaskIDFromParam).Result()
	//
	//if err != nil {
	//	glog.Errorf("Error During Get Job Data From Redis %s", err)
	//	os.Exit(-1)
	//}
	isValid, taskType := util.GetTaskTypeFromFileName(taskId)

	if !isValid {

		glog.Errorf("file name %s is not valid", taskId)
		os.Exit(1)

	}

	lcm.TaskType = taskType
	os.Setenv(util.TaskTypeDeltaDelete, taskType) //for delete

	if lcm.TaskType == util.TaskTypeFull {

		lcm.AzTaskResultMgr = dbmgr.GetTaskEventHubClient(os.Getenv(Full_EventHub_ConnStr), os.Getenv(Full_EventHub_Task))
		lcm.AzJobResultMgr = dbmgr.GetJobEventHubClient(os.Getenv(Full_EventHub_ConnStr), os.Getenv(Full_EventHub_Job))
	}

	if lcm.TaskType == util.TaskTypeDeltaUpsert {

		lcm.AzTaskResultMgr = dbmgr.GetTaskEventHubClient(os.Getenv(Delta_EventHub_ConnStr), os.Getenv(Delta_EventHub_UpsertTask))
		lcm.AzJobResultMgr = dbmgr.GetJobEventHubClient(os.Getenv(Delta_EventHub_ConnStr), os.Getenv(Delta_EventHub_UpsertJob))
	}

	if lcm.TaskType == util.TaskTypeDeltaDelete {
		lcm.AzTaskResultMgr = dbmgr.GetTaskEventHubClient(os.Getenv(Delta_EventHub_ConnStr), os.Getenv(Delta_EventHub_DeleteTask))
		lcm.AzJobResultMgr = dbmgr.GetJobEventHubClient(os.Getenv(Delta_EventHub_ConnStr), os.Getenv(Delta_EventHub_DeleteJob))
	}

	// fileName := taskId + ".json"
	// jsonFile, err := os.Open(fileName)
	// if err != nil {
	// 	panic(err)
	// }
	// defer jsonFile.Close()
	// byteValue, err := ioutil.ReadAll(jsonFile)
	// if err != nil {
	// 	panic(err)
	// }

	// newJob := make([]dbmgr.JobData, 0, Task_Max_File)
	// err = json.Unmarshal(byteValue, &newJob)
	// if err != nil {
	// 	glog.Error("Error During Get Job Data From Json file %s", err)
	// }

	//for performance tuning
	// Open the JSON file
	jsonFile, err := os.Open(taskId + ".json")
	if err != nil {
		panic(err)
	}
	defer jsonFile.Close()
	// Create a JSON Decoder
	decoder := json.NewDecoder(jsonFile)
	// Read opening bracket for the array of JSON objects
	_, err = decoder.Token()
	if err != nil {
		glog.Error("Error reading opening bracket: ", err)
		panic(err)
	}
	// Initialize the slice with capacity
	newJob := make([]dbmgr.JobDataB2B, 0, 1000)
	// Decode each JSON object
	for decoder.More() {
		var job dbmgr.JobDataB2B
		err := decoder.Decode(&job)
		if err != nil {
			glog.Error("Error decoding JSON: ", err)
			panic(err)
		}
		newJob = append(newJob, job)
	}
	// Check for the closing bracket of the array
	_, err = decoder.Token()
	if err != nil {
		glog.Error("Error reading closing bracket: ", err)
		panic(err)
	}

	//620 err = json.Unmarshal([]byte(jobs), &newJob)
	//if err != nil {
	//	glog.Error("Error During Get Job Data From Redis %s", err)
	//}
	// bkName = os.Getenv("SOURCE_BUCKET")
	// //bkRegion = todo
	connStr := os.Getenv("AZ_BLOB_CONN_STRING")
	if len(connStr) == 0 {
		glog.Error("blob connection string is empty")
	}
	//appInfo := util.ParseAzBlobConnString(connStr)
	appInfo := make(map[string]string)
	appInfo["AccountName"] = os.Getenv("AZ_BLOB_STORAGE_ACCOUNT")
	appInfo["ContainerName"] = os.Getenv("AZ_BLOB_CONTAINER")
	appInfo["AccountURL"] = connStr

	blobClient := azblobmgr.GetAzBlobContainerClientDest(appInfo)
	lcm.AzBlobMgrDest = blobClient
	//blobClient.GenerateSAS(appInfo)
	//blobClient.GetOrCreateSAS(appInfo)
	destUrl := blobClient.URLBuilderNoSAS(azBlobContainer, bkName)
	lcm.AzBlobURLPrefix = blobClient.URLPrefixBuilder(azBlobContainer, bkName)

	lcm.AzBlobContainer = azBlobContainer

	// lcm.AzBlobMgr = blobClient
	// blobClient.GenerateSAS(appInfo)
	// destUrl := blobClient.URLBuilder(azBlobContainer, bkName)

	// sourceUrlPresfix := fmt.Sprintf("https://%s.s3.%s.amazonaws.com", bkName, bkRegion)
	// lcm.SourceURLPrefix = sourceUrlPresfix + "/"
	// lcm.AzBlobURLPrefix = blobClient.URLPrefixBuilder(azBlobContainer, bkName)

	wholeMap := make(map[string]dbmgr.JobDataB2B, len(newJob))
	resultMap := make(map[string]dbmgr.JobDataB2B, len(newJob))
	lcm.SkippedDelNum = 0

	for _, jobItem := range newJob {
		trimStringFieldsB2B(&jobItem)
		//bkName = jobItem.BucketName
		//bkRegion = jobItem.Region

		if len(sourceUrlPresfix) == 0 {

			sourceUrlPresfix = fmt.Sprintf("https://%s.s3.%s.amazonaws.com", bkName, bkRegion)
			endpoint = fmt.Sprintf("https://s3.%s.amazonaws.com", bkRegion)
			if strings.Contains(jobItem.SourceUrl, Aliyun_OSS) {
				sourceUrlPresfix = fmt.Sprintf("https://%s.oss-%s.aliyuncs.com", bkName, bkRegion)
				endpoint = fmt.Sprintf("https://oss-%s.aliyuncs.com", bkRegion)
			}

			glog.Infof("Using endpoint: %s", endpoint)
		}

		if lcm.TaskType == util.TaskTypeDeltaDelete {

			//isExistObj, err := checkS3FileExist(bkName, jobItem.FilePath, jobItem.Region, endpoint)
			isExistObj, err := isSingleBlobForDelSrc(jobItem.FilePath)

			if isExistObj { //S3 exists
				//if checkS3FileExistWithRetry(bkName, jobItem.FilePath, jobItem.Region) {

				wholeMap[fmt.Sprintf("%s/%s", azBlobContainer, jobItem.FilePath)] = jobItem

				lcm.SkippedDelNum = lcm.SkippedDelNum + 1

				lcm.SubmitJobLogBeforeCmdB2B(jobItem, Job_Delete_Message_SkippedSuccess, Job_Status_SkippedSuccess)

			} else if err == nil || err.Error() == Error_Authorization_Failure {

				glog.Infof("File %s is not existing in S3.", jobItem.SourceUrl)

				wholeMap[fmt.Sprintf("%s/%s", azBlobContainer, jobItem.FilePath)] = jobItem

				isValidBlob, err := isSingleBlobForDelDest(jobItem.FilePath)
				if isValidBlob {

					resultMap[fmt.Sprintf("%s/%s", azBlobContainer, jobItem.FilePath)] = jobItem
				} else if err != nil {

					lcm.SubmitJobLogBeforeCmdB2B(jobItem, err.Error(), Job_Status_Failed)
				} else {

					lcm.SubmitJobLogBeforeCmdB2B(jobItem, Job_Delete_Message_SkippedFail, Job_Status_Failed)
				}
			} else {

				lcm.SubmitJobLogBeforeCmdB2B(jobItem, err.Error(), Job_Status_Failed)
			}

		} else {

			resultMap[fmt.Sprintf("%s/%s", jobItem.ContainerName, jobItem.FilePath)] = jobItem

			wholeMap[fmt.Sprintf("%s/%s", jobItem.ContainerName, jobItem.FilePath)] = jobItem
		}

	}

	// if len(wholeMap) == lcm.NonDeleteNum {

	// 	//lcm.SubmitTaskResultToeEventHub(lcm.NonDeleteNum, lcm.NonDeleteNum, lcm.NonDeleteNum, 0, 0)
	// 	lcm.AzTaskResultMgr.AzTaskResultData.NumberOfFiles = lcm.NonDeleteNum
	// 	lcm.AzTaskResultMgr.SetAzTaskResult(lcm.TaskIDFromParam, lcm.TaskType, true, lcm.NonDeleteNum, lcm.NonDeleteNum, 0, 0, util.CalFileSize(wholeMap))
	// }

	lcm.FileSliceB2B = wholeMap

	listFile := createTempFile(resultMap, lcm.TaskIDFromParam)

	// connStr := os.Getenv("AZ_BLOB_CONN_STRING")
	// if len(connStr) == 0 {
	// 	glog.Error("blob connection string is empty")
	// }
	// appInfo := util.ParseAzBlobConnString(connStr)
	// blobClient := azblobmgr.GetAzBlobContainerClient(appInfo)
	// Set Az Queue Client if ENV ADD_TO_QUEUE is true
	// if ok, _ := strconv.ParseBool(os.Getenv("ADD_TO_QUEUE")); ok {
	// 	blobClient.SetAzQueueClient(appInfo)
	// }

	// lcm.AzBlobMgr = blobClient
	// blobClient.GenerateSAS(appInfo)
	// destUrl := blobClient.URLBuilder(azBlobContainer, bkName)
	for index, item := range os.Args {
		//630
		if index == 1 && item == Copy {
			result = append(result, Copy)
			continue
		}
		if index == 1 && item == Remove {
			result = append(result, Remove)
			continue
		}
		switch item {
		case "-t", "-j", taskId, jobId:
			continue
		default:
			result = append(result, item)
		}

	}

	//sourceUrlPresfix := fmt.Sprintf("https://%s.s3.%s.amazonaws.com", bkName, bkRegion)
	//sourceUrlPresfix := fmt.Sprintf("https://%s.oss-%s.aliyuncs.com", bkName, bkRegion)
	lcm.SourceURLPrefix = sourceUrlPresfix + "/"
	// lcm.AzBlobURLPrefix = blobClient.URLPrefixBuilder(azBlobContainer, bkName)

	if lcm.TaskType == util.TaskTypeDeltaDelete {
		// lcm.AzTaskResultMgr = dbmgr.GetTaskEventHubClient(os.Getenv(Delta_EventHub_ConnStr), os.Getenv(Delta_EventHub_DeleteTask))
		// lcm.AzJobResultMgr = dbmgr.GetJobEventHubClient(os.Getenv(Delta_EventHub_ConnStr), os.Getenv(Delta_EventHub_DeleteJob))
		result = append(result, destUrl, "--recursive=true",
			"--list-of-files",
			listFile)
	} else {
		result = append(result, sourceUrlPresfix, destUrl,
			"--list-of-files",
			listFile)
	}

	return result
	// []string{
	// 	"azcopy",
	// 	"copy",
	// 	fmt.Sprintf("https://%s.s3.%s.amazonaws.com", bkName, bkRegion),
	// 	destUrl,
	// 	"--list-of-files",
	// 	listFile,
	// }
}

func (lcm *lifecycleMgr) GetOverwriteIfNew() bool {
	return lcm.OverwriteIfNew
}

func (lcm *lifecycleMgr) GetJobDataFromMapWithKeyS3(key string) dbmgr.JobDataS3 {
	return lcm.FileSliceS3[key]
}

func (lcm *lifecycleMgr) GetJobDataFromMapWithKeyB2B(key string) dbmgr.JobDataB2B {
	return lcm.FileSliceB2B[key]
}

// SetSingleJobDataToCosmos add single job result data into cosmos db MigrationTasks container with random partition key.
// The range of random partition key is specified in lcm.CosmosPKRange.
//620 func (lcm *lifecycleMgr) SetSingleJobDataToCosmos(fileName string, status int, message string, statusCode int) {
// 	bucketName := os.Getenv("SOURCE_BUCKET")
// 	jobObj := lcm.FileSlice[fmt.Sprintf("%s/%s", bucketName, fileName)]
// 	rand.Seed(time.Now().UnixNano())
// 	jobData := dbmgr.JobResultData{
// 		Id:             uuid.New().String(),
// 		JobId:          lcm.JobIDFromParam,
// 		TaskId:         lcm.TaskIDFromParam,
// 		Bucket:         bucketName,
// 		FileName:       fileName,
// 		VersionId:      jobObj.VersionId,
// 		Status:         status,
// 		Message:        message,
// 		PartitionKey:   strconv.Itoa(rand.Intn(lcm.CosmosPKRange)),
// 		OperationTime:  time.Now().UnixNano(),
// 		ModifyTime:     jobObj.ModifyTime,
// 		StorageClass:   jobObj.StorageClass,
// 		StatusCode:     statusCode,
// 		Size:           jobObj.Size,
// 		SourceURL:      lcm.SourceURLPrefix + fileName,
// 		DestinationURL: lcm.AzBlobURLPrefix + fileName,
// 	}
// 	lcm.CosmosJobMgr.SetJobResult(jobData, lcm.AzBlobMgr)

// }

// SetJobCosmosMgr add new job cosmos client into lcm
// func (lcm *lifecycleMgr) SetJobCosmosMgr() {
// 	lcm.CosmosJobMgr = dbmgr.GetCosmosClient(lcm.YamlConfig)
// }

// deleteRedisJobsWhenFinished delete redis key when task is finished, only work in non debug mode.
func (lcm *lifecycleMgr) deleteRedisJobsWhenFinished() {
	//if lcm.YamlConfig.DEBUG_MODE != 1 {
	if os.Getenv(Debug_Mode) != "1" {
		err := lcm.JobDataRedisClient.Del(context.TODO(), lcm.TaskIDFromParam).Err()
		if err != nil {
			glog.Errorf("Cannot Delete Job "+lcm.TaskIDFromParam+" in Redis: err", err)
		}
	}
}

// SubmitJobResult Submit job result with multiple goroutine.
// DEPRECATED
// 620 func (lcm *lifecycleMgr) SubmitJobResult(builder func(format OutputFormat) string, exitCode ExitCode) {
// 	cos := dbmgr.GetCosmosClient(lcm.YamlConfig)

// 	wg := new(sync.WaitGroup)
// 	wg.Add(len(lcm.JobResultData))

// 	for _, item := range lcm.JobResultData {
// 		go cos.SetBulkJobResult(item, wg, lcm.AzBlobMgr)
// 	}

// 	wg.Wait()
// 	lcm.Exit(builder, exitCode)
// }

// SetTaskExecutionStartTime add a task start time
func (lcm *lifecycleMgr) SetTaskExecutionStartTime() {
	//dbmgr.SetTaskStartTimeWithRestAPI(lcm.YamlConfig, lcm.TaskIDFromParam, lcm.JobIDFromParam)
	lcm.AzTaskResultMgr.AzTaskResultData.ExecutionStartTime = time.Now().Format(time.RFC3339)

}

// SubmitTaskResultToCosmos calculates the fail rate of task and submit task result into cosmos db.
//620 func (lcm *lifecycleMgr) SubmitTaskResultToCosmos(totalJobs int, skippedJob int, finishedJob int, failedJob int, totalTransferBytes int) {
// 	os.Remove(path.Join(os.TempDir(), lcm.TaskIDFromParam))

// 	var totalSize int64 = 0
// 	for _, value := range lcm.FileSlice {
// 		totalSize += value.Size
// 	}

// 	if len(lcm.FileSlice) != skippedJob+finishedJob+failedJob {
// 		glog.Warningf("The number of Skipped Job %d + Finished Job %d + Failed Job %d does not match Total Number of Jobs %d", skippedJob, finishedJob, failedJob, totalJobs)
// 		dbmgr.SetTaskResultWithRestAPI(lcm.YamlConfig, lcm.TaskIDFromParam, lcm.JobIDFromParam, false, skippedJob, finishedJob, failedJob, totalTransferBytes, totalSize)
// 	}

// 	if totalJobs < 10000 {
// 		if failedJob > util.JobFailRateCalculator(totalJobs, 0.001) {
// 			dbmgr.SetTaskResultWithRestAPI(lcm.YamlConfig, lcm.TaskIDFromParam, lcm.JobIDFromParam, false, skippedJob, finishedJob, failedJob, totalTransferBytes, totalSize)
// 		} else {
// 			dbmgr.SetTaskResultWithRestAPI(lcm.YamlConfig, lcm.TaskIDFromParam, lcm.JobIDFromParam, true, skippedJob, finishedJob, failedJob, totalTransferBytes, totalSize)
// 		}
// 	} else {
// 		if failedJob > util.JobFailRateCalculator(totalJobs, 0.0001) {
// 			dbmgr.SetTaskResultWithRestAPI(lcm.YamlConfig, lcm.TaskIDFromParam, lcm.JobIDFromParam, false, skippedJob, finishedJob, failedJob, totalTransferBytes, totalSize)
// 		} else {
// 			dbmgr.SetTaskResultWithRestAPI(lcm.YamlConfig, lcm.TaskIDFromParam, lcm.JobIDFromParam, true, skippedJob, finishedJob, failedJob, totalTransferBytes, totalSize)
// 		}
// 	}

// 	lcm.deleteRedisJobsWhenFinished()
// }

// ConvertRedisDataToMinioObjectInfo read job data from lcm.FileSlice and convert it into Minio ObjectInfo.
func (lcm *lifecycleMgr) ConvertRedisDataToMinioObjectInfo(bucketName string, objKey string) (minio.ObjectInfo, error) {
	mapKey := fmt.Sprintf("%s/%s", bucketName, objKey)

	if value, ok := lcm.FileSliceS3[mapKey]; ok {
		lastModifyTime, _ := time.Parse(time.RFC3339, value.ModifyTime)
		metadataHeader := http.Header{}
		if len(value.VersionId) != 0 {
			metadataHeader.Set("X-Amz-Version-Id", value.VersionId)
		}
		return minio.ObjectInfo{
			ETag:         value.Etag,
			Key:          objKey,
			LastModified: lastModifyTime,
			Size:         int64(value.Size),
			ContentType:  "",
			Metadata:     metadataHeader,
			Owner: struct {
				DisplayName string `json:"name"`
				ID          string `json:"id"`
			}{},
			StorageClass: value.StorageClass,
			Err:          nil,
		}, nil
	}
	return minio.ObjectInfo{}, errors.New(fmt.Sprintf("Cannot Find bucket %s and obj key %s", bucketName, objKey))
}

func trimStringFieldsB2B(job *dbmgr.JobDataB2B) {
	v := reflect.ValueOf(job).Elem() // Getting the value that the pointer 'job' points to
	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i) // Check if the field is of type string
		if field.Kind() == reflect.String {
			trimmedValue := strings.TrimSpace(field.String()) // Trim the field
			field.SetString(trimmedValue)                     // Set back the trimmed value
		}
	}
}

func trimStringFieldsS3(job *dbmgr.JobDataS3) {
	v := reflect.ValueOf(job).Elem() // Getting the value that the pointer 'job' points to
	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i) // Check if the field is of type string
		if field.Kind() == reflect.String {
			trimmedValue := strings.TrimSpace(field.String()) // Trim the field
			field.SetString(trimmedValue)                     // Set back the trimmed value
		}
	}
}

func (lcm *lifecycleMgr) SubmitJobLogBeforeCmdS3(jobItem dbmgr.JobDataS3, message string, jobStatus int) {

	jobData := dbmgr.AzJobResultData{
		Id:             uuid.New().String(),
		JobId:          lcm.JobIDFromParam,
		TaskId:         lcm.TaskIDFromParam,
		TaskType:       lcm.TaskType,
		Message:        message,
		OperationTime:  time.Now().Format(time.RFC3339),
		ModifyTime:     jobItem.ModifyTime,
		StorageClass:   jobItem.StorageClass,
		StatusCode:     jobStatus,
		Size:           jobItem.Size,
		SourceURL:      jobItem.SourceUrl,
		DestinationURL: lcm.AzBlobURLPrefix + jobItem.FilePath,
	}
	lcm.AzJobResultMgr.SetAzJobResult(jobData)

}

func (lcm *lifecycleMgr) SubmitJobLogBeforeCmdB2B(jobItem dbmgr.JobDataB2B, message string, jobStatus int) {

	jobData := dbmgr.AzJobResultData{
		Id:            uuid.New().String(),
		JobId:         lcm.JobIDFromParam,
		TaskId:        lcm.TaskIDFromParam,
		TaskType:      lcm.TaskType,
		Message:       message,
		OperationTime: time.Now().Format(time.RFC3339),
		ModifyTime:    jobItem.ModifyTime,
		AccessTier:    jobItem.SrcTier,
		//StorageClass:   jobItem.StorageClass,
		StatusCode:     jobStatus,
		Size:           jobItem.Size,
		SourceURL:      jobItem.SourceUrl,
		DestinationURL: fmt.Sprintf("%s%s", lcm.AzBlobURLPrefix, jobItem.FileName),
	}
	lcm.AzJobResultMgr.SetAzJobResult(jobData)

}

// checkS3FileExistWithRetry checks for the existence of a file in an S3 bucket with a specified number of retries.
func checkS3FileExistWithRetry(bucketName, fileName, region string) bool {
	var maxRetries = 3
	sess := session.Must(session.NewSessionWithOptions(session.Options{SharedConfigState: session.SharedConfigEnable}))
	sess.Config.Region = &region
	s3svc := s3.New(sess)
	for attempt := 1; attempt <= maxRetries; attempt++ {
		_, err := s3svc.HeadObject(&s3.HeadObjectInput{
			Bucket: aws.String(bucketName), Key: aws.String(fileName)})
		if err == nil {
			return true
		}

		if strings.Contains(err.Error(), "send request failed") && attempt < maxRetries {
			time.Sleep(time.Duration(attempt+1) * time.Second)
			// Exponential backoff could be considered here
		}
	}
	return false
}

func checkS3FileExist(bucketName, fileName, region string, endpoint string) (bool, error) {
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		Config: aws.Config{
			Endpoint: aws.String(endpoint),
			Region:   aws.String(region),
			// Set custom retry count and backoff strategy.
			MaxRetries: aws.Int(3),
			Retryer:    aws.UseServiceDefaultRetries,
		}, SharedConfigState: session.SharedConfigEnable,
	}))
	s3svc := s3.New(sess)
	// Define the operation to be retried in case of specific errors.
	var err error
	for attempt := 1; attempt <= 5; attempt++ {
		_, err = s3svc.HeadObject(&s3.HeadObjectInput{
			Bucket: aws.String(bucketName), Key: aws.String(fileName),
		})
		if err != nil {
			awsErr, ok := err.(awserr.Error)
			glog.Errorf("Error occurred while checking file existence: %v; AWS Error Code: %s", err, awsErr.Code())

			if !ok {
				return false, nil
			} // Check for specific error codes and decide whether to retry.
			if awsErr.Code() == Error_Request_Error {

				time.Sleep(time.Duration(attempt+1) * time.Second)

			} else if awsErr.Code() == Error_Forbidden {

				glog.Errorf(Error_Forbidden+": Bucket Name: %s, File Name: %s", bucketName, fileName)
				return false, errors.New(Error_Authorization_Failure)

			} else if awsErr.Code() == Error_NotFound || strings.Contains(err.Error(), "404") {

				return false, nil

			} else {
				return false, err
			}

		} else {
			return true, nil
		}
	}
	glog.Errorf(Error_Request_Error+": Bucket Name: %s, File Name: %s", bucketName, fileName)
	return false, errors.New(Error_Http_Request_Failed)
}

// func checkS3FileExist(bucketName, fileName string, region string) bool {
// 	sess := session.Must(session.NewSessionWithOptions(session.Options{SharedConfigState: session.SharedConfigEnable}))
// 	sess.Config.Region = &region
// 	//sess := session.Must(session.NewSession(&aws.Config{ Region: aws.String(region), }))
// 	s3svc := s3.New(sess)
// 	_, err := s3svc.HeadObject(&s3.HeadObjectInput{Bucket: aws.String(bucketName), Key: aws.String(fileName)})
// 	return err == nil
// }

// func checkS3FileExist(bucketName, fileName string, region string, maxRetries int) bool {
// 	sess, err := session.NewSessionWithOptions(session.Options{
// 		Config: aws.Config{
// 			Region:  aws.String(region),
// 			Retryer: aws.DefaultRetryer{NumMaxRetries: maxRetries}},
// 		SharedConfigState: session.SharedConfigEnable,
// 	})
// 	if err != nil {
// 		log.Printf("Failed to create session: %s\n", err)
// 		return false
// 	}
// 	s3svc := s3.New(sess)
// 	retryCount := 0
// 	for {
// 		_, err := s3svc.HeadObject(&s3.HeadObjectInput{
// 			Bucket: aws.String(bucketName), Key: aws.String(fileName),
// 		})
// 		if err == nil {
// 			return true
// 		}
// 		retryCount++
// 		if retryCount > maxRetries {
// 			glog.Debugf("Max retries exceeded for file: %s in bucket: %s\n", fileName, bucketName)
// 			return false
// 		}
// 		glog.Debugf("Retrying to check file: %s. Attempt: %d\n", fileName, retryCount)
// 		time.Sleep(time.Second * 2)
// 		// Exponential backoff could be implemented here
// 	}
// }

// func isSingleBlobForDel(blobName string) (isBlob bool) {

// 	blobURL := lcm.AzBlobMgr.ContainerClient.NewBlobURL(blobName)
// 	// Here, we try to fetch the blob's properties
// 	_, err := blobURL.GetProperties(context.Background(), azblob.BlobAccessConditions{}, azblob.ClientProvidedKeyOptions{})
// 	if err != nil {
// 		// An error occurred, possibly because the blob does not exist or due to other reasons like network issues.
// 		if azblob.StorageErrorCodeType(err.Error()) == azblob.StorageErrorCodeBlobNotFound {
// 			glog.Infof("Blob %s does not exist in the container \n", lcm.AzBlobURLPrefix+blobName)
// 			return false
// 		} else {
// 			glog.Infof("Blob %s is not found: %s\n", lcm.AzBlobURLPrefix+blobName, err.Error())
// 			return false
// 		}

// 	} else {

// 		//glog.Debugf("Blob %s exists in the container \n", lcm.AzBlobURLPrefix+blobName)
// 		return true
// 	}

// 	// if err.Error() == "HTTP request failed" {

// 	// 	//Todo retry two more times

// 	// }
// }

func isArchivedSingleBlobDest(blobName string) (bool, error) {
	blobURL := lcm.AzBlobMgrDest.ContainerClientDest.NewBlobClient(blobName)
	maxRetries := 3
	baseDelay := time.Second * 2

	for attempt := 0; attempt < maxRetries; attempt++ {
		props, err := blobURL.GetProperties(context.Background(), nil)
		if err != nil {
			if strings.Contains(err.Error(), Error_Http_Request_Failed) {
				delay := time.Duration(math.Pow(2, float64(attempt))) * baseDelay
				glog.Debugf("Network error on attempt %d for blob %s: %s. Retrying in %v...\n", attempt+1, blobURL.URL(), err.Error(), delay)
				time.Sleep(delay)
				continue
			} else if strings.Contains(err.Error(), Error_Authorization_Failure) {
				glog.Errorf(Error_Authorization_Failure+": %s", blobURL.URL())
				return false, err
			} else if strings.Contains(err.Error(), Error_BlobNotFound) {

				glog.Debugf("Blob %s is not found: %s\n", blobURL.URL(), err.Error())
				return false, err
			} else {

				glog.Debugf("Blob %s checking: is Archived? error occurred\n", blobURL.URL())
				return false, nil
			}
		}

		if azblob.AccessTierType(*props.AccessTier) == azblob.AccessTierArchive {
			return true, nil
		}

		return false, nil
	}

	errMsg := fmt.Sprintf("Final attempt still resulted in network error for blob %s; operation failed.\n", blobURL.URL())
	glog.Error(errMsg)
	return false, errors.New(errMsg)
}

func isArchivedSingleBlobSrc(blobName string) (bool, error) {
	// Check if the source is GCP
	if strings.Contains(lcm.SourceURLPrefix, "storage.cloud.google.com") || strings.Contains(lcm.SourceURLPrefix, "storage.googleapis.com") {
		// GCP Check
		return false, nil
		/*
			bucketName := lcm.AzBlobContainer // In GCP case, AzBlobContainer holds the bucket name
			// blobName here is likely the relative path or full path.
			// However, LCM logic constructs keys in wholeMap: fmt.Sprintf("%s/%s%s", jobItem.ContainerName, jobItem.FilePath, jobItem.FileName)

			// Ensure we have a valid key for GCP
			// The caller passes `fmt.Sprintf("%s%s", jobItem.FilePath, jobItem.FileName)`

			// We need a GCP client. Since lifeCycleMgr is a singleton and might not have a GCP client initialized explicitly for this check,
			// we should try to create one or reuse if possible.
			// However, isArchivedSingleBlobSrc is called during scanning/job generation.

			ctx := context.Background()
			client, err := CreateGCPClient(ctx)
			if err != nil {
				glog.Errorf("Failed to create GCP client for archive check: %v", err)
				return false, err
			}
			defer client.Close()

			// blobName passed here is usually just the object key (file path + file name)
			// But let's verify if it contains container name or handled by caller.
			// In `handleBlobArchiving`: isArchivedSingleBlobSrc(fmt.Sprintf("%s%s", jobItem.FilePath, jobItem.FileName))
			// So it is the Object Key.

			objHandle := client.Bucket(bucketName).Object(blobName)
			attrs, err := objHandle.Attrs(ctx)
			if err != nil {
				if err == gcpUtils.ErrObjectNotExist {
					return false, errors.New(Error_BlobNotFound)
				}
				glog.Errorf("Failed to get GCP object attributes for %s: %v", blobName, err)
				return false, err
			}

			// GCP Archive classes: ARCHIVE
			if attrs.StorageClass == "ARCHIVE" {
				return true, nil
			}
			return false, nil
		*/
	}

	blobURL := lcm.AzBlobMgrSrc.ContainerClientSrc.NewBlobClient(blobName)
	maxRetries := 3
	baseDelay := time.Second * 2

	for attempt := 0; attempt < maxRetries; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		props, err := blobURL.GetProperties(ctx, nil)
		if err != nil {
			if strings.Contains(err.Error(), Error_Http_Request_Failed) {
				delay := time.Duration(math.Pow(2, float64(attempt))) * baseDelay
				glog.Debugf("Network error on attempt %d for blob %s: %s. Retrying in %v...\n", attempt+1, blobURL.URL(), err.Error(), delay)
				time.Sleep(delay)
				continue
			} else if strings.Contains(err.Error(), Error_Authorization_Failure) {
				glog.Errorf(Error_Authorization_Failure+": %s", blobURL.URL())
				return false, err
			} else if strings.Contains(err.Error(), Error_BlobNotFound) {

				glog.Debugf("Blob %s is not found: %s\n", blobURL.URL(), err.Error())
				return false, err
			} else {

				glog.Debugf("Blob %s checking: is Archived? error occurred\n", blobURL.URL())
				return false, nil
			}
		}

		if azblob.AccessTierType(*props.AccessTier) == azblob.AccessTierArchive {
			return true, nil
		}

		return false, nil
	}

	errMsg := fmt.Sprintf("Final attempt still resulted in network error for blob %s; operation failed.\n", blobURL.URL())
	glog.Error(errMsg)
	return false, errors.New(errMsg)
}

// func isSingleBlobForDelSrc(blobName string) (bool, error) {

// 	blobURL := lcm.AzBlobMgrSrc.ContainerClientSrc.NewBlobClient(blobName)
// 	maxRetries := 3
// 	baseDelay := time.Second * 2

// 	for attempt := 0; attempt < maxRetries; attempt++ {
// 		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
// 		defer cancel()

// 		_, err := blobURL.GetProperties(ctx, nil)
// 		if err != nil {
// 			if strings.Contains(err.Error(), Error_Http_Request_Failed) {
// 				delay := time.Duration(math.Pow(2, float64(attempt))) * baseDelay
// 				glog.Debugf("Network error on attempt %d for blob %s: %s. Retrying in %v...\n", attempt+1, blobURL.URL(), err.Error(), delay)
// 				time.Sleep(delay)
// 				continue
// 			} else if strings.Contains(err.Error(), Error_Authorization_Failure) {
// 				glog.Errorf(Error_Authorization_Failure+": %s", blobURL.URL())
// 				return false, errors.New(Error_Authorization_Failure)
// 			} else if strings.Contains(err.Error(), Error_BlobNotFound404) {
// 				glog.Debugf("Blob %s is not found: %s\n", blobURL.URL(), err.Error())
// 				return false, nil
// 			} else {
// 				glog.Debugf("Blob %s checking: error occurred\n", blobURL.URL())
// 				return false, nil
// 			}
// 		} else {
// 			return true, nil
// 		}
// 	}

// 	errMsg := fmt.Sprintf("Final attempt still resulted in network error for blob %s; operation failed.\n", blobURL.URL())
// 	glog.Error(errMsg)
// 	return false, errors.New(errMsg)
// }

func isSingleBlobForDelSrc(blobName string) (bool, error) {

	blobURL := lcm.AzBlobMgrSrc.ContainerClientSrc.NewBlobClient(blobName)
	maxRetries := 3
	baseDelay := time.Second * 2

	for attempt := 0; attempt < maxRetries; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		_, err := blobURL.GetProperties(ctx, nil)
		if err != nil {
			if strings.Contains(err.Error(), Error_Http_Request_Failed) {
				delay := time.Duration(math.Pow(2, float64(attempt))) * baseDelay
				glog.Debugf("Network error on attempt %d for blob %s: %s. Retrying in %v...\n", attempt+1, blobURL.URL(), err.Error(), delay)
				time.Sleep(delay)
				continue
			} else if strings.Contains(err.Error(), Error_Authorization_Failure) {
				glog.Errorf(Error_Authorization_Failure+": %s", blobURL.URL())
				return false, errors.New(Error_Authorization_Failure)
			} else if strings.Contains(err.Error(), Error_BlobNotFound404) {
				glog.Debugf("Blob %s is not found: %s\n", blobURL.URL(), err.Error())
				return false, nil
			} else {
				glog.Debugf("Blob %s checking: error occurred\n", blobURL.URL())
				return false, nil
			}
		} else {
			return true, nil
		}
	}

	errMsg := fmt.Sprintf("Final attempt still resulted in network error for blob %s; operation failed.\n", blobURL.URL())
	glog.Error(errMsg)
	return false, errors.New(errMsg)
}

func isSingleGCSObjectForDelSrc(objectName string) (bool, error) {
	// For GCP GCS to Azure Blob scenario, check if the object exists in GCS
	if !strings.Contains(lcm.SourceURLPrefix, "storage.cloud.google.com") && !strings.Contains(lcm.SourceURLPrefix, "storage.googleapis.com") {
		// Not GCS, return false or handle differently
		return false, errors.New("Not a GCS source")
	}

	// Create GCS client
	ctx := context.Background()
	gcpClient, err := gcpUtils.NewClient(ctx)
	if err != nil {
		glog.Errorf("Failed to create GCS client: %v", err)
		return false, err
	}
	//defer gcpClient.Close()

	// Extract bucket name from SourceURLPrefix
	// SourceURLPrefix is like https://storage.cloud.google.com/bucket/path/
	u, err := url.Parse(lcm.SourceURLPrefix)
	if err != nil {
		glog.Errorf("Failed to parse SourceURLPrefix: %v", err)
		return false, err
	}
	pathParts := strings.Split(strings.Trim(u.Path, "/"), "/")
	if len(pathParts) < 1 {
		glog.Errorf("Invalid SourceURLPrefix path: %s", u.Path)
		return false, errors.New("Invalid bucket name")
	}
	bucketName := pathParts[0]

	// Get object attributes
	_, err = gcpClient.Bucket(bucketName).Object(objectName).Attrs(ctx)
	if err != nil {
		if e, ok := err.(*googleapi.Error); ok && e.Code == 404 || strings.Contains(err.Error(), "storage: object doesn't exist") {
			glog.Debugf("GCS object %s does not exist", objectName)
			return false, nil
		} else {
			glog.Errorf("Error checking GCS object %s: %v", objectName, err)
			return false, err
		}
	}

	return true, nil
}

func isSingleBlobForDelDest(blobName string) (bool, error) {
	blobURL := lcm.AzBlobMgrDest.ContainerClientDest.NewBlobClient(blobName)
	maxRetries := 3
	baseDelay := time.Second * 2

	for attempt := 0; attempt < maxRetries; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		_, err := blobURL.GetProperties(ctx, nil)
		if err != nil {
			if strings.Contains(err.Error(), Error_Http_Request_Failed) {
				delay := time.Duration(math.Pow(2, float64(attempt))) * baseDelay
				glog.Debugf("Network error on attempt %d for blob %s: %s. Retrying in %v...\n", attempt+1, blobURL.URL(), err.Error(), delay)
				time.Sleep(delay)
				continue
			} else if strings.Contains(err.Error(), Error_Authorization_Failure) {
				glog.Errorf(Error_Authorization_Failure+": %s", blobURL.URL())
				return false, errors.New(Error_Authorization_Failure)
			} else if strings.Contains(err.Error(), Error_BlobNotFound404) {
				glog.Debugf("Blob %s is not found: %s\n", blobURL.URL(), err.Error())
				return false, nil
			} else {
				glog.Debugf("Blob %s checking: error occurred\n", blobURL.URL())
				return false, nil
			}
		} else {
			return true, nil
		}
	}

	errMsg := fmt.Sprintf("Final attempt still resulted in network error for blob %s; operation failed.\n", blobURL.URL())
	glog.Error(errMsg)
	return false, errors.New(errMsg)
}

//630
// func checkFileExists(sess *session.Session, bucketName, key string) bool {
// 	svc := s3.New(sess) _, err := svc.HeadObject(&s3.HeadObjectInput{
// 		 Bucket: aws.String(bucketName), Key: aws.String(key),
// 		 }) return err == nil
// }

// GetJobDataFromjson read task detail information from json file.

//func (lcm *lifecycleMgr) GetJobDataFromjsonfile(taskId string, jobId string) []string {
//	var bkName, sourceUrl, containerName string
//	result := []string{}
//	newJob := []dbmgr.JobData{}
//	jsonfile := taskId + ".json"
//	jsonFile, err := os.Open(jsonfile)
//	if err != nil {
//		glog.Error("Error During Get Job Data From Json %s", err)
//	}
//	defer jsonFile.Close()
//	byteValue, _ := ioutil.ReadAll(jsonFile)
//	err = json.Unmarshal(byteValue, &newJob)
//	if err != nil {
//		glog.Error("Error During Get Job Data From Json %s", err)
//	}
//	resultMap := make(map[string]dbmgr.JobData)
//	for _, item := range newJob {
//		sourceUrl = item.SourceUrl
//		re := regexp.MustCompile(`https://(\w+).blob.core.windows.net/(\w+)/(.*)`)
//		matches := re.FindStringSubmatch(sourceUrl)
//		if len(matches) != 0 {
//			bkName = matches[1]
//			containerName = matches[2]
//			item.ContainerName = containerName
//			item.StAccount = bkName
//		}
//		resultMap[fmt.Sprintf("%s/%s", item.StAccount, item.FilePath)] = item
//	}
//	lcm.FileSlice = resultMap
//	listFile := createTempFile(resultMap, lcm.TaskIDFromParam)
//	os.Setenv("AZ_SOURCE_STORAGE_ACCOUNT", bkName)
//	connStr := os.Getenv("AZ_BLOB_CONN_STRING")
//	if len(connStr) == 0 {
//		glog.Error("blob connection string is empty")
//	}
//	appInfo := util.ParseAzBlobConnString(connStr)
//	blobClient := azblobmgr.GetAzBlobContainerClient(appInfo, containerName)
//	//Set Az Queue Client if ENV ADD_TO_QUEUE is true
//	//if ok, _ := strconv.ParseBool(os.Getenv("ADD_TO_QUEUE")); ok {
//	//	blobClient.SetAzQueueClient(appInfo)
//	//}
//	lcm.AzBlobMgr = blobClient
//	blobClient.GenerateSAS(appInfo)
//	destUrl := blobClient.URLBuilder(containerName, bkName)
//	for index, item := range os.Args {
//		if index == 1 && item != "copy" {
//			result = append(result, "copy")
//			continue
//		}
//		switch item {
//		case "-t", "-j", taskId, jobId:
//			continue
//		default:
//			result = append(result, item)
//		}
//	}
//	sourceUrl = fmt.Sprintf("%s", sourceUrl)
//	lcm.SourceURLPrefix = sourceUrl
//	lcm.AzBlobURLPrefix = blobClient.URLPrefixBuilder(containerName, bkName)
//	result = append(result, sourceUrl, destUrl,
//		"--list-of-files",
//		listFile)
//	return result
//
//}
//
//// createTempFile create temp file for azcopy to read.
//}

func createTempFile(data map[string]dbmgr.JobDataB2B, taskId string) string {
	tempFile := path.Join(os.TempDir(), taskId)
	file, err := os.OpenFile(tempFile, os.O_WRONLY|os.O_CREATE, 0777)
	writer := bufio.NewWriter(file)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	tVal, tq := "", ""
	for _, val := range data {

		//tq, err = url.QueryUnescape(val.FilePath)
		tq, err = url.QueryUnescape(val.FileName)
		if err != nil {
			tVal = val.FileName
		} else {
			tVal = tq
		}
		writer.WriteString(tVal + "\n")
	}
	writer.Flush()
	return tempFile
}

// SetSingleTaskDataToCosmos add single task result data into eventhub  with random partition key.
func (lcm *lifecycleMgr) SetSingleJobDataToEventhubS3(fileName string, status int, message string, statusCode int) {
	//taskType := os.Getenv("TASKTYPE_FROM_FILENAME")
	bucketName := os.Getenv("SOURCE_BUCKET")
	//for debug
	//bucketName := "emr-testing-lgj"
	taskType := lcm.TaskType
	var jobObj dbmgr.JobDataS3
	//jobObj := lcm.FileSlice[fmt.Sprintf("%s/%s", bucketName, fileName)]
	if taskType == util.TaskTypeDeltaDelete {

		jobObj = lcm.FileSliceS3[fileName]
	} else {

		jobObj = lcm.FileSliceS3[fmt.Sprintf("%s/%s", bucketName, fileName)]
	}

	rand.Seed(time.Now().UnixNano())
	jobData := dbmgr.AzJobResultData{
		Id:             uuid.New().String(),
		JobId:          lcm.JobIDFromParam,
		TaskId:         lcm.TaskIDFromParam,
		TaskType:       taskType,
		Message:        message,
		OperationTime:  time.Now().Format(time.RFC3339),
		ModifyTime:     jobObj.ModifyTime,
		StorageClass:   jobObj.StorageClass,
		StatusCode:     statusCode,
		Size:           jobObj.Size,
		SourceURL:      lcm.SourceURLPrefix + jobObj.FilePath,
		DestinationURL: lcm.AzBlobURLPrefix + fileName,
	}
	lcm.AzJobResultMgr.SetAzJobResult(jobData)
}

func (lcm *lifecycleMgr) CacheAzblobCre(azblobcre azblob.Credential) {
	lcm.azblobCre = azblobcre
}

func (lcm *lifecycleMgr) GetCacheAzblobCre() azblob.Credential {
	return lcm.azblobCre
}

func truncateMessage(message string) string {
	if len(message) > 166 {
		return message[:166]
	}
	return message
}

func (lcm *lifecycleMgr) SetSingleJobDataToEventhubB2B(fileName string, status int, message string, statusCode int) {
	//taskType := os.Getenv("TASKTYPE_FROM_FILENAME")
	//bucketName := os.Getenv("SOURCE_BUCKET")
	//targetContainerName := os.Getenv("AZ_BLOB_CONTAINER")
	//for debug
	//bucketName := "emr-testing-lgj"
	taskType := lcm.TaskType
	var jobObj dbmgr.JobDataB2B
	//jobObj := lcm.FileSlice[fmt.Sprintf("%s/%s", bucketName, fileName)]
	// if taskType == util.TaskTypeDeltaDelete {

	// 	jobObj = lcm.FileSliceB2B[fileName]
	// } else {

	// 	jobObj = lcm.FileSliceB2B[fileName]
	// }

	if strings.HasPrefix(fileName, lcm.AzBlobContainer) {

		jobObj = lcm.FileSliceB2B[fileName]

	} else if strings.HasPrefix(fileName, "https://") {

		blobkey, error := util.ParseContainerURL(fileName)

		if error != nil {
			glog.Error("Error During Parse Container URL %s", error)
		} else {
			jobObj = lcm.FileSliceB2B[blobkey]
		}
	} else {
		jobObj = lcm.FileSliceB2B[fmt.Sprintf("%s%s", lcm.AzBlobContainer, fileName)]
	}

	rand.Seed(time.Now().UnixNano())
	jobData := dbmgr.AzJobResultData{
		Id:             uuid.New().String(),
		JobId:          lcm.JobIDFromParam,
		TaskId:         lcm.TaskIDFromParam,
		TaskType:       taskType,
		Message:        truncateMessage(message),
		OperationTime:  time.Now().Format(time.RFC3339),
		ModifyTime:     jobObj.ModifyTime,
		AccessTier:     jobObj.SrcTier,
		StatusCode:     statusCode,
		Size:           jobObj.Size,
		SourceURL:      lcm.SourceURLPrefix + jobObj.FileName,
		DestinationURL: lcm.AzBlobURLPrefix + jobObj.FileName,
	}
	lcm.AzJobResultMgr.SetAzJobResult(jobData)
}

// SubmitTaskResultToeEventHub calculates the fail rate of task and submit task result to eventhub.

func (lcm *lifecycleMgr) SubmitTaskResultToEventHubS3(totalJobs int, skippedJob int, finishedJob int, failedJob int, totalTransferBytes int) {
	os.Remove(path.Join(os.TempDir(), lcm.TaskIDFromParam))

	var totalSize int64 = 0
	for _, value := range lcm.FileSliceS3 {
		totalSize += value.Size
	}

	var numCopyJob int = skippedJob + finishedJob + failedJob
	var numJobOfTask int = len(lcm.FileSliceS3)
	var numOfNonCopy int = numJobOfTask - numCopyJob - lcm.SkippedDelNum //for copy is 0
	failedJob = failedJob + numOfNonCopy
	skippedJob = skippedJob + lcm.SkippedDelNum

	if numJobOfTask > numCopyJob && numOfNonCopy > 0 && lcm.TaskType != util.TaskTypeDeltaDelete {

		lcm.AzTaskResultMgr.AzTaskResultData.NumberOfFiles = numOfNonCopy
		glog.Debugf("The number of Skipped Job %d + Finished Job %d + Failed Job %d does not match Total Number of Jobs %d", skippedJob, finishedJob, failedJob, totalJobs)
		lcm.AzTaskResultMgr.SetAzTaskResultAsync(lcm.TaskIDFromParam, lcm.TaskType, false, 0, 0, numOfNonCopy, 0, 0)
	}

	var isComplete bool = false

	if failedJob <= util.NewJobFailRateCalculator(numJobOfTask) {

		isComplete = true
	} else {

		isComplete = false
	}

	lcm.AzTaskResultMgr.AzTaskResultData.NumberOfFiles = numJobOfTask
	isComplete = true
	lcm.AzTaskResultMgr.SetAzTaskResultAsync(lcm.TaskIDFromParam, lcm.TaskType, isComplete, skippedJob, finishedJob, failedJob, totalTransferBytes, totalSize)

}

func (lcm *lifecycleMgr) SubmitTaskResultToEventHubB2B(totalJobs int, skippedJob int, finishedJob int, failedJob int, totalTransferBytes int) {
	os.Remove(path.Join(os.TempDir(), lcm.TaskIDFromParam))

	var totalSize int64 = 0
	for _, value := range lcm.FileSliceB2B {
		totalSize += value.Size
	}

	var numCopyJob int = skippedJob + finishedJob + failedJob
	var numJobOfTask int = len(lcm.FileSliceB2B)
	var numOfNonCopy int = numJobOfTask - numCopyJob - lcm.SkippedDelNum //for copy is 0
	failedJob = failedJob + numOfNonCopy
	skippedJob = skippedJob + lcm.SkippedDelNum

	if numJobOfTask > numCopyJob && numOfNonCopy > 0 && numCopyJob > 0 && lcm.TaskType != util.TaskTypeDeltaDelete {

		lcm.AzTaskResultMgr.AzTaskResultData.NumberOfFiles = numOfNonCopy
		glog.Debugf("The number of Skipped Job %d + Finished Job %d + Failed Job %d does not match Total Number of Jobs %d", skippedJob, finishedJob, failedJob, totalJobs)
		lcm.AzTaskResultMgr.SetAzTaskResultAsync(lcm.TaskIDFromParam, lcm.TaskType, false, 0, 0, numOfNonCopy, 0, 0)
	}

	var isComplete bool = false

	if failedJob <= util.NewJobFailRateCalculator(numJobOfTask) {

		isComplete = true
	} else {

		isComplete = false
	}

	lcm.AzTaskResultMgr.AzTaskResultData.NumberOfFiles = numJobOfTask
	isComplete = true
	lcm.AzTaskResultMgr.SetAzTaskResultAsync(lcm.TaskIDFromParam, lcm.TaskType, isComplete, skippedJob, finishedJob, failedJob, totalTransferBytes, totalSize)

}

// should be started in a single go routine
func (lcm *lifecycleMgr) watchInputs() {
	consoleReader := bufio.NewReader(os.Stdin)
	for {
		// sleep for a bit, the option might be enabled later
		if !lcm.allowWatchInput {
			time.Sleep(time.Microsecond * 500)
			continue
		}

		// reads input until the first occurrence of \n in the input,
		input, err := consoleReader.ReadString('\n')
		if err != nil {
			continue
		}

		// remove spaces before/after the content
		msg := strings.TrimSpace(input)
		timeReceived := time.Now()

		select {
		case <-lcm.waitForUserResponse:
			lcm.inputQueue <- userInput{timeReceived: timeReceived, content: msg}
			continue
		default:
		}

		var req LCMMsgReq
		if lcm.allowCancelFromStdIn && strings.EqualFold(msg, "cancel") {
			lcm.cancelChannel <- os.Interrupt
		} else if lcm.e2eAllowAwaitContinue && strings.EqualFold(msg, "continue") {
			close(lcm.e2eContinueChannel)
		} else if lcm.e2eAllowAwaitOpen && strings.EqualFold(msg, "open") {
			close(lcm.e2eAllowOpenChannel)
		} else if err := json.Unmarshal([]byte(msg), &req); err == nil { //json string
			lcm.Info(fmt.Sprintf("Received request for %s with timeStamp %s", req.MsgType, req.TimeStamp.String()))
			var msgType LCMMsgType
			if err := msgType.Parse(req.MsgType); err != nil {
				lcm.Info(fmt.Sprintf("Discarding incorrect message: %s.", req.MsgType))
				continue
			}

			switch msgType {
			case ELCMMsgType.CancelJob():
				lcm.cancelChannel <- os.Interrupt
			default:
				m := NewLCMMsg()
				m.Req = &req
				lcm.msgHandlerChannel <- m

				//wait till the message is completed
				<-m.respChan
				lcm.Response(*m.Resp)
			}
		} else {
			lcm.Info("Discarding incorrectly formatted input message")
		}
	}
}

// get the answer to a question that was asked at a certain time
// only user input after the specified time is returned to make sure that we are getting the right answer to our question
// NOTE: to ask a question, go through Prompt, to guarantee that only 1 question is asked at a time
func (lcm *lifecycleMgr) getInputAfterTime(time time.Time) string {
	for {
		msg := <-lcm.inputQueue

		// keep reading until we find an input that came in after the user specified time
		if msg.timeReceived.After(time) {
			return msg.content
		}

		// otherwise keep waiting as it's possible that the user has not typed it in yet
	}
}

func (lcm *lifecycleMgr) EnableInputWatcher() {
	lcm.allowWatchInput = true
}

func (lcm *lifecycleMgr) EnableCancelFromStdIn() {
	lcm.allowCancelFromStdIn = true
}

func (lcm *lifecycleMgr) ClearEnvironmentVariable(variable EnvironmentVariable) {
	_ = os.Setenv(variable.Name, "")
}

func (lcm *lifecycleMgr) SetOutputFormat(format OutputFormat) {
	lcm.outputFormat = format
}

func (lcm *lifecycleMgr) checkAndStartCPUProfiling() {
	// CPU Profiling add-on. Set AZCOPY_PROFILE_CPU to enable CPU profiling,
	// the value AZCOPY_PROFILE_CPU indicates the path to save CPU profiling data.
	// e.g. export AZCOPY_PROFILE_CPU="cpu.prof"
	// For more details, please refer to https://golang.org/pkg/runtime/pprof/
	cpuProfilePath := lcm.GetEnvironmentVariable(EEnvironmentVariable.ProfileCPU())
	if cpuProfilePath != "" {
		lcm.Info(fmt.Sprintf("pprof start CPU profiling, and saving profiling data to: %q", cpuProfilePath))
		f, err := os.Create(cpuProfilePath)
		if err != nil {
			lcm.Error(fmt.Sprintf("Fail to create file for CPU profiling, %v", err))
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			lcm.Error(fmt.Sprintf("Fail to start CPU profiling, %v", err))
		}
	}
}

func (lcm *lifecycleMgr) checkAndStopCPUProfiling() {
	// Stop CPU profiling if there is ongoing CPU profiling.
	pprof.StopCPUProfile()
}

func (lcm *lifecycleMgr) checkAndTriggerMemoryProfiling() {
	// Memory Profiling add-on. Set AZCOPY_PROFILE_MEM to enable memory profiling,
	// the value AZCOPY_PROFILE_MEM indicates the path to save memory profiling data.
	// e.g. export AZCOPY_PROFILE_MEM="mem.prof"
	// For more details, please refer to https://golang.org/pkg/runtime/pprof/
	memProfilePath := lcm.GetEnvironmentVariable(EEnvironmentVariable.ProfileMemory())
	if memProfilePath != "" {
		lcm.Info(fmt.Sprintf("pprof start memory profiling, and saving profiling data to: %q", memProfilePath))
		f, err := os.Create(memProfilePath)
		if err != nil {
			lcm.Error(fmt.Sprintf("Fail to create file for memory profiling, %v", err))
		}
		runtime.GC()
		if err := pprof.WriteHeapProfile(f); err != nil {
			lcm.Error(fmt.Sprintf("Fail to start memory profiling, %v", err))
		}
		if err := f.Close(); err != nil {
			lcm.Info(fmt.Sprintf("Fail to close memory profiling file, %v", err))
		}
	}
}

func (lcm *lifecycleMgr) Init(o OutputBuilder) {
	lcm.msgQueue <- outputMessage{
		msgContent: o(lcm.outputFormat),
		msgType:    eOutputMessageType.Init(),
	}
}

func (lcm *lifecycleMgr) Progress(o OutputBuilder) {
	messageContent := ""
	if o != nil {
		messageContent = o(lcm.outputFormat)
	}

	lcm.msgQueue <- outputMessage{
		msgContent: messageContent,
		msgType:    eOutputMessageType.Progress(),
	}
}

func (lcm *lifecycleMgr) Info(msg string) {

	msg = lcm.logSanitizer.SanitizeLogMessage(msg) // sometimes error-like text comes through Info, before the final "we've failed, please stop now" signal comes to Error. So we sanitize in both places.

	infoMsg := fmt.Sprintf("INFO: %v", msg)

	lcm.msgQueue <- outputMessage{
		msgContent: infoMsg,
		msgType:    eOutputMessageType.Info(),
	}
}

func (lcm *lifecycleMgr) Prompt(message string, details PromptDetails) ResponseOption {

	expectedInputChannel := make(chan string, 1)
	lcm.msgQueue <- outputMessage{
		msgContent:    message,
		msgType:       eOutputMessageType.Prompt(),
		inputChannel:  expectedInputChannel,
		promptDetails: details,
	}

	// Request watchInputs() to wait for response from user
	lcm.waitForUserResponse <- true

	// block until input comes from the user
	rawResponse := <-expectedInputChannel

	// match the given response against one of the options we gave
	for _, option := range details.ResponseOptions {
		// in case the user misunderstood and typed full response type instead, we still tolerate it
		// e.g. instead of "y", user typed "Yes"
		if strings.EqualFold(option.ResponseString, rawResponse) ||
			strings.EqualFold(option.UserFriendlyResponseType, rawResponse) {
			return option
		}
	}

	// nothing matched our options, assume default behavior (up to whoever that called Prompt)
	// we don't re-prompt the user since this makes the integration with Stg Exp more complex
	return EResponseOption.Default()
}

func (lcm *lifecycleMgr) Dryrun(o OutputBuilder) {
	dryrunMessage := ""
	if o != nil {
		dryrunMessage = o(lcm.outputFormat)
	}

	lcm.msgQueue <- outputMessage{
		msgContent: dryrunMessage,
		msgType:    eOutputMessageType.Dryrun(),
	}
}

// TODO minor: consider merging with Exit
func (lcm *lifecycleMgr) Error(msg string) {

	msg = lcm.logSanitizer.SanitizeLogMessage(msg)

	// Check if need to do memory profiling, and do memory profiling accordingly before azcopy exits.
	lcm.checkAndTriggerMemoryProfiling()

	// Check if there is ongoing CPU profiling, and stop CPU profiling.
	lcm.checkAndStopCPUProfiling()

	lcm.msgQueue <- outputMessage{
		msgContent: msg,
		msgType:    eOutputMessageType.Error(),
		exitCode:   EExitCode.Error(),
	}

	// stall forever until the success message is printed and program exits
	lcm.SurrenderControl()
}

func (lcm *lifecycleMgr) Exit(o OutputBuilder, applicationExitCode ExitCode) {
	if applicationExitCode != EExitCode.NoExit() {
		// Check if need to do memory profiling, and do memory profiling accordingly before azcopy exits.
		lcm.checkAndTriggerMemoryProfiling()

		// Check if there is ongoing CPU profiling, and stop CPU profiling.
		lcm.checkAndStopCPUProfiling()
	}

	messageContent := ""
	if o != nil {
		messageContent = o(lcm.outputFormat)
	}

	lcm.msgQueue <- outputMessage{
		msgContent: messageContent,
		msgType:    eOutputMessageType.EndOfJob(),
		exitCode:   applicationExitCode,
	}

	if applicationExitCode != EExitCode.NoExit() {
		// stall forever until the success message is printed and program exits
		lcm.SurrenderControl()
	}
}

func (lcm *lifecycleMgr) Response(resp LCMMsgResp) {

	var respMsg string

	if lcm.outputFormat == EOutputFormat.Json() {
		m, err := json.Marshal(resp)
		respMsg = string(m)
		PanicIfErr(err)
	} else {
		respMsg = fmt.Sprintf("INFO: %v", resp.Value.String())
	}

	respMsg = lcm.logSanitizer.SanitizeLogMessage(respMsg)

	lcm.msgQueue <- outputMessage{
		msgContent: respMsg,
		msgType:    eOutputMessageType.Response(),
	}
}

// this is used by commands that wish to stall forever to wait for the operations to complete
func (lcm *lifecycleMgr) SurrenderControl() {
	// stall forever
	select {}
}

func (lcm *lifecycleMgr) RegisterCloseFunc(closeFunc func()) {
	lcm.closeFunc = closeFunc
}

func (lcm *lifecycleMgr) processOutputMessage() {
	// this function constantly pulls out message to output
	// and pass them onto the right handler based on the output format
	for {
		msgToPrint := <-lcm.msgQueue

		if shouldQuietMessage(msgToPrint, lcm.OutputVerbosityType) {
			lcm.processNoneOutput(msgToPrint)
			continue
		}
		switch lcm.outputFormat {
		case EOutputFormat.Json():
			lcm.processJSONOutput(msgToPrint)
		case EOutputFormat.Text():
			lcm.processTextOutput(msgToPrint)
		case EOutputFormat.None():
			lcm.processNoneOutput(msgToPrint)
		default:
			panic("unimplemented output format")
		}
	}
}

func (lcm *lifecycleMgr) processNoneOutput(msgToOutput outputMessage) {
	if msgToOutput.msgType == eOutputMessageType.Error() {
		lcm.closeFunc()
		os.Exit(int(EExitCode.Error()))
	} else if msgToOutput.shouldExitProcess() {
		lcm.closeFunc()
		os.Exit(int(msgToOutput.exitCode))
	}

	// ignore all other outputs
	return
}

func (lcm *lifecycleMgr) processJSONOutput(msgToOutput outputMessage) {
	msgType := msgToOutput.msgType
	questionTime := time.Now()

	// simply output the json message
	// we assume the msgContent is already formatted correctly
	fmt.Println(GetJsonStringFromTemplate(newJsonOutputTemplate(msgType, msgToOutput.msgContent,
		msgToOutput.promptDetails)))

	// exit if needed
	if msgToOutput.shouldExitProcess() {
		lcm.closeFunc()
		os.Exit(int(msgToOutput.exitCode))
	} else if msgType == eOutputMessageType.Prompt() {
		// read the response to the prompt and send it back through the channel
		msgToOutput.inputChannel <- lcm.getInputAfterTime(questionTime)
	}
}

func (lcm *lifecycleMgr) processTextOutput(msgToOutput outputMessage) {
	// when a new line needs to overwrite the current line completely
	// we need to make sure that if the new line is shorter, we properly erase everything from the current line
	var matchLengthWithSpaces = func(curLineLength, newLineLength int) {
		if dirtyLeftover := curLineLength - newLineLength; dirtyLeftover > 0 {
			for i := 0; i < dirtyLeftover; i++ {
				fmt.Print(" ")
			}
		}
	}

	switch msgToOutput.msgType {
	case eOutputMessageType.Error(), eOutputMessageType.EndOfJob():
		// simply print and quit
		// if no message is intended, avoid adding new lines
		if msgToOutput.msgContent != "" {
			fmt.Println("\n" + msgToOutput.msgContent)
		}
		if msgToOutput.shouldExitProcess() {
			lcm.closeFunc()
			os.Exit(int(msgToOutput.exitCode))
		}

	case eOutputMessageType.Progress():
		fmt.Print("\r")                   // return carriage back to start
		fmt.Print(msgToOutput.msgContent) // print new progress

		// it is possible that the new progress status is somehow shorter than the previous one
		// in this case we must erase the left over characters from the previous progress
		matchLengthWithSpaces(len(lcm.progressCache), len(msgToOutput.msgContent))

		lcm.progressCache = msgToOutput.msgContent

	case eOutputMessageType.Init(), eOutputMessageType.Info(), eOutputMessageType.Dryrun(), eOutputMessageType.Response():
		if lcm.progressCache != "" { // a progress status is already on the last line
			// print the info from the beginning on current line
			fmt.Print("\r")
			fmt.Print(msgToOutput.msgContent)

			// it is possible that the info is shorter than the progress status
			// in this case we must erase the left over characters from the progress status
			matchLengthWithSpaces(len(lcm.progressCache), len(msgToOutput.msgContent))

			// print the previous progress status again, so that it's on the last line
			fmt.Print("\n")
			fmt.Print(lcm.progressCache)
		} else {
			fmt.Println(msgToOutput.msgContent)
		}
	case eOutputMessageType.Prompt():
		questionTime := time.Now()

		if lcm.progressCache != "" { // a progress status is already on the last line
			// print the prompt from the beginning on current line
			fmt.Print("\r")
			fmt.Print(msgToOutput.msgContent)

			// it is possible that the prompt is shorter than the progress status
			// in this case we must erase the left over characters from the progress status
			matchLengthWithSpaces(len(lcm.progressCache), len(msgToOutput.msgContent))

		} else {
			fmt.Print(msgToOutput.msgContent)
		}

		// example output: Please confirm with: [Y] Yes  [N] No  [A] Yes for all  [L] No for all
		fmt.Print(" Please confirm with:")
		for _, option := range msgToOutput.promptDetails.ResponseOptions {
			fmt.Printf(" [%s] %s ", strings.ToUpper(option.ResponseString), option.UserFriendlyResponseType)
		}

		// read the response to the prompt and send it back through the channel
		msgToOutput.inputChannel <- lcm.getInputAfterTime(questionTime)
	}
}

// for the lifecycleMgr to babysit a job, it must be given a controller to get information about the job
type WorkController interface {
	Cancel(mgr LifecycleMgr)                                        // handle to cancel the work
	ReportProgressOrExit(mgr LifecycleMgr) (totalKnownCount uint32) // print the progress status, optionally exit the application if work is done
}

// AllowReinitiateProgressReporting must be called before running an cleanup job, to allow the initiation of that job's
// progress reporting to begin
func (lcm *lifecycleMgr) AllowReinitiateProgressReporting() {
	atomic.StoreInt32(&lcm.waitEverCalled, 0)
}

// isInteractive indicates whether the application was spawned by an actual user on the command
func (lcm *lifecycleMgr) InitiateProgressReporting(jc WorkController) {
	if !atomic.CompareAndSwapInt32(&lcm.waitEverCalled, 0, 1) {
		return
	}

	// this go routine never returns
	// it will terminate the whole process eventually when the work is complete
	go func() {
		const progressFrequencyThreshold = 1000000
		var oldCount, newCount uint32
		wait := 2 * time.Second
		lastFetchTime := time.Now().Add(-wait) // So that we start fetching time immediately

		// cancelChannel will be notified when os receives os.Interrupt and os.Kill signals
		signal.Notify(lcm.cancelChannel, os.Interrupt, os.Kill)

		cancelCalled := false

		doCancel := func() {
			cancelCalled = true
			lcm.Info("Cancellation requested. Beginning clean shutdown...")
			jc.Cancel(lcm)
		}

		for {
			select {
			case <-lcm.cancelChannel:
				doCancel()
				continue // to exit on next pass through loop
			case <-lcm.doneChannel:

				newCount = jc.ReportProgressOrExit(lcm)
				lastFetchTime = time.Now()
			case <-time.After(wait):
				if time.Since(lastFetchTime) >= wait {
					newCount = jc.ReportProgressOrExit(lcm)
					lastFetchTime = time.Now()
				}
			}

			if newCount >= progressFrequencyThreshold && !cancelCalled {
				// report less on progress  - to save on the CPU costs of doing so and because, if there are this many files,
				// its going to be a long job anyway, so no need to report so often
				wait = 2 * time.Minute
				if oldCount < progressFrequencyThreshold {
					lcm.Info(fmt.Sprintf("Reducing progress output frequency to %v, because there are over %d files", wait, progressFrequencyThreshold))
				}
			}

			oldCount = newCount
		}
	}()
}

func (lcm *lifecycleMgr) GetEnvironmentVariable(env EnvironmentVariable) string {
	value := os.Getenv(env.Name)
	if value == "" {
		return env.DefaultValue
	}
	return value
}

func (lcm *lifecycleMgr) AddUserAgentPrefix(userAgent string) string {
	prefix := lcm.GetEnvironmentVariable(EEnvironmentVariable.UserAgentPrefix())
	if len(prefix) > 0 {
		userAgent = prefix + " " + userAgent
	}

	return userAgent
}

func (_ *lifecycleMgr) awaitChannel(ch chan struct{}, timeout time.Duration) {
	select {
	case <-ch:
	case <-time.After(timeout):
	}
}

// E2EAwaitContinue is used in case where a developer want's to debug AzCopy by attaching to the running process,
// before it starts doing any actual work.
func (lcm *lifecycleMgr) E2EAwaitContinue() {
	lcm.e2eAllowAwaitContinue = true // not technically gorountine safe (since its shared state) but its consistent with EnableInputWatcher
	lcm.EnableInputWatcher()
	lcm.awaitChannel(lcm.e2eContinueChannel, time.Minute)
}

// E2EAwaitAllowOpenFiles is used in cases where we want to artificially produce a pause between enumeration and sending
// of the first file, for test purposes. (It only achieves that effect when the total file count is <= size of one job part).
// Does not pause at all, unless the feature has been enabled with a command-line flag.
func (lcm *lifecycleMgr) E2EAwaitAllowOpenFiles() {
	lcm.awaitChannel(lcm.e2eAllowOpenChannel, 5*time.Minute)
}

func (lcm *lifecycleMgr) E2EEnableAwaitAllowOpenFiles(enable bool) {
	if enable {
		lcm.e2eAllowAwaitOpen = true // not technically gorountine safe (since its shared state) but its consistent with EnableInputWatcher
		lcm.EnableInputWatcher()
	} else {
		close(lcm.e2eAllowOpenChannel) // so that E2EAwaitAllowOpenFiles will instantly return every time
	}
}

// Fetching `AZCOPY_DISABLE_SYSLOG` from the environment variables and
// setting `disableSyslog` flag in LifeCycleManager to avoid Env Vars Lookup redundantly
func (lcm *lifecycleMgr) SetForceLogging() {
	disableSyslog, err := strconv.ParseBool(lcm.GetEnvironmentVariable(EEnvironmentVariable.DisableSyslog()))
	if err != nil {
		// By default, we'll retain the current behaviour. i.e. To log in Syslog/WindowsEventLog if not specified by the user
		disableSyslog = false
	}
	lcm.disableSyslog = disableSyslog
}

func (lcm *lifecycleMgr) IsForceLoggingDisabled() bool {
	return lcm.disableSyslog
}

func (lcm *lifecycleMgr) DownloadToTempPath() bool {
	ret, err := strconv.ParseBool(lcm.GetEnvironmentVariable(EEnvironmentVariable.DownloadToTempPath()))
	if err != nil {
		// By default we'll download to temp path
		ret = true
	}
	return ret
}

func (lcm *lifecycleMgr) MsgHandlerChannel() <-chan *LCMMsg {
	return lcm.msgHandlerChannel
}

func (lcm *lifecycleMgr) ReportAllJobPartsDone() {
	lcm.doneChannel <- true
}

func (lcm *lifecycleMgr) SetOutputVerbosity(mode OutputVerbosity) {
	lcm.OutputVerbosityType = mode
}

// captures the common logic of exiting if there's an expected error
func PanicIfErr(err error) {
	if err != nil {
		panic(err)
	}
}

func shouldQuietMessage(msgToOutput outputMessage, quietMode OutputVerbosity) bool {
	messageType := msgToOutput.msgType

	switch quietMode {
	case EOutputVerbosity.Default():
		return false
	case EOutputVerbosity.Essential():
		return messageType == eOutputMessageType.Progress() || messageType == eOutputMessageType.Info() || messageType == eOutputMessageType.Prompt()
	case EOutputVerbosity.Quiet():
		return true
	default:
		return false
	}
}

// func blobExists(container, blobName string) bool {
// 	//for delete
// 	client, err := storage.NewClientFromConnectionString(os.Getenv("AZ_BLOB_CONN_STRING"))
// 	if err != nil {
// 		fmt.Println("Error creating client:", err)
// 		return
// 	}

// 	blobService := client.GetFileService()
// 	exists, err := blobService.BlobExists(container, blobName)
// 	if err != nil {
// 		fmt.Println("Error checking if blob exists:", err)
// 		return false
// 	}
// 	return exists
// }

// SetSingleTaskDataToCosmos add single task result data into eventhub  with random partition key.
// func (lcm *lifecycleMgr) SetSingleJobDataToEventhub(fileName string, status int, message string, statusCode int) {
// 	//taskType := os.Getenv("TASKTYPE_FROM_FILENAME")
// 	bucketName := os.Getenv("SOURCE_BUCKET")
// 	//for debug
// 	//bucketName := "emr-testing-lgj"
// 	taskType := lcm.TaskType
// 	//jobObj := lcm.FileSlice[fmt.Sprintf("%s/%s", bucketName, fileName)]
// 	jobObj := lcm.FileSlice[fmt.Sprintf("%s%s", bucketName, fileName)]
// 	rand.Seed(time.Now().UnixNano())
// 	jobData := dbmgr.AzJobResultData{
// 		Id:             uuid.New().String(),
// 		JobId:          lcm.JobIDFromParam,
// 		TaskId:         lcm.TaskIDFromParam,
// 		TaskType:       taskType,
// 		Message:        message,
// 		OperationTime:  time.Now().Format(time.RFC3339),
// 		ModifyTime:     jobObj.ModifyTime,
// 		StorageClass:   jobObj.StorageClass,
// 		StatusCode:     statusCode,
// 		Size:           jobObj.Size,
// 		SourceURL:      lcm.SourceURLPrefix,
// 		DestinationURL: lcm.AzBlobURLPrefix + fileName,
// 	}
// 	lcm.AzJobResultMgr.SetAzJobResult(jobData)
// }

// // SubmitTaskResultToeEventHub calculates the fail rate of task and submit task result to eventhub.

// func (lcm *lifecycleMgr) SubmitTaskResultToeEventHub(totalJobs int, skippedJob int, finishedJob int, failedJob int, totalTransferBytes int) {
// 	os.Remove(path.Join(os.TempDir(), lcm.TaskIDFromParam))

// 	var totalSize int64 = 0
// 	for _, value := range lcm.FileSlice {
// 		totalSize += value.Size
// 	}

// 	// if totalJobs < 10000 {
// 	// 	if failedJob < util.JobFailRateCalculator(totalJobs, 0.001) {
// 	// 		isComplete = true
// 	// 	}
// 	// } else {

// 	// 	if failedJob < util.JobFailRateCalculator(totalJobs, 0.0001) {
// 	// 		isComplete = true
// 	// 	}
// 	// }

// 	var numCopyJob int = skippedJob + finishedJob + failedJob
// 	var numJobOfTask int = len(lcm.FileSlice)
// 	var numOfNonCopy int = numJobOfTask - numCopyJob
// 	failedJob = failedJob + numOfNonCopy

// 	if numJobOfTask != numCopyJob {

// 		lcm.AzTaskResultMgr.AzTaskResultData.NumberOfFiles = numOfNonCopy
// 		glog.Warningf("The number of Skipped Job %d + Finished Job %d + Failed Job %d does not match Total Number of Jobs %d", skippedJob, finishedJob, failedJob, totalJobs)
// 		lcm.AzTaskResultMgr.SetAzTaskResult(lcm.TaskIDFromParam, lcm.TaskType, false, 0, 0, numOfNonCopy, 0, 0)
// 	}

// 	var isComplete bool = false

// 	if failedJob <= util.NewJobFailRateCalculator(numJobOfTask) {

// 		isComplete = true
// 	} else {

// 		isComplete = false
// 	}

// 	lcm.AzTaskResultMgr.AzTaskResultData.NumberOfFiles = numJobOfTask
// 	lcm.AzTaskResultMgr.SetAzTaskResult(lcm.TaskIDFromParam, lcm.TaskType, isComplete, skippedJob, finishedJob, failedJob+numOfNonCopy, totalTransferBytes, totalSize)

// 	// if totalJobs < 10000 {
// 	// 	if failedJob > util.JobFailRateCalculator(totalJobs, 0.001) {
// 	// 		lcm.AzTaskResultMgr.SetAzTaskResult(lcm.TaskIDFromParam, lcm.TaskType, false, skippedJob, finishedJob, failedJob, totalTransferBytes, totalSize)
// 	// 	} else {
// 	// 		lcm.AzTaskResultMgr.SetAzTaskResult(lcm.TaskIDFromParam, lcm.TaskType, true, skippedJob, finishedJob, failedJob, totalTransferBytes, totalSize)
// 	// 	}
// 	// } else {
// 	// 	if failedJob > util.JobFailRateCalculator(totalJobs, 0.0001) {
// 	// 		lcm.AzTaskResultMgr.SetAzTaskResult(lcm.TaskIDFromParam, lcm.TaskType, false, skippedJob, finishedJob, failedJob, totalTransferBytes, totalSize)

// 	// 	} else {
// 	// 		lcm.AzTaskResultMgr.SetAzTaskResult(lcm.TaskIDFromParam, lcm.TaskType, true, skippedJob, finishedJob, failedJob, totalTransferBytes, totalSize)

// 	// 	}

// 	// 	//630 lcm.deleteRedisJobsWhenFinished()
// 	// }
// }

// GetAzureKeys get keys (bucket name, storage credential, aws credential, cosmos db credential, and cosmos db partition key range for MigrationTasks)
func (lcm *lifecycleMgr) GetAzureKeys(jobId string) {

	var gcs_service_account_key string

	var eventhubconnStrFull, eventhubNameFullTask, eventhubNameFullJob string
	var eventhubconnStrDelta, eventhubNameUpsertTask, eventhubNameUpsertJob string
	var eventhubNameDeleteTask, eventhubNameDeleteJob string

	if lcm.DebugMode == 1 {
		// Read from CSV
		file, err := os.Open("Redis Dump_20250917.csv")
		if err != nil {
			glog.Error("Cannot open Redis Dump_20250917.csv: ", err)
			os.Exit(-1)
		}
		defer file.Close()

		reader := csv.NewReader(file)
		// Skip header if necessary
		records, err := reader.ReadAll()
		if err != nil {
			glog.Error("Cannot read CSV: ", err)
			os.Exit(-1)
		}

		kvMap := make(map[string]string)
		for _, record := range records {
			if len(record) >= 2 {
				kvMap[record[0]] = record[1]
			}
		}

		//storageConn = kvMap[storageAccount]
		// aws_access_key_id = kvMap[awsKeyId]
		// aws_secret_access_key = kvMap[awsSecretKeyId]
		gcs_service_account_key = kvMap["gcp-service-account-key"]

		os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "gcp-project-479500-2546852f4a0f.json")

		eventhubconnStrFull = kvMap["EventHubNameSpace--Full--FullyQualifiedNamespace"]
		eventhubNameFullTask = kvMap["EventHubNameSpace--Full--EventHubName--Task"]
		eventhubNameFullJob = kvMap["EventHubNameSpace--Full--EventHubName--Job"]

		eventhubconnStrDelta = kvMap["EventHubNameSpace--Delta--FullyQualifiedNamespace"]
		eventhubNameUpsertTask = kvMap["EventHubNameSpace--Delta--EventHubName--UpsertTask"]
		eventhubNameUpsertJob = kvMap["EventHubNameSpace--Delta--EventHubName--UpsertJob"]

		eventhubNameDeleteTask = kvMap["EventHubNameSpace--Delta--EventHubName--DeleteTask"]
		eventhubNameDeleteJob = kvMap["EventHubNameSpace--Delta--EventHubName--DeleteJob"]
	} else {
		//Decrypt redis cache with AES 128
		redisHost, redisPWD, redisSSL, err := util.GetAzureRedisCredential("RedisCache", jobId)
		if err != nil {
			lcm.GetKeyVault()
		}

		if len(redisHost) != 0 {
			os.Setenv(Redis_Host, redisHost)
			os.Setenv(Redis_PWD, redisPWD)
			os.Setenv("REDIS_SSL", redisSSL)
		}
		//TODO exit if redisHost is empty

		//bucketName := os.Getenv("SOURCE_BUCKET")
		//storageAccount := os.Getenv("AZ_BLOB_STORAGE_ACCOUNT")

		//lcm.AzBlobContainer = storageAccount
		//b2b awsKeyId := fmt.Sprintf("%s-%s", bucketName, "AWSACCESSKEYID")
		//b2b awsSecretKeyId := fmt.Sprintf("%s-%s", bucketName, "AWSSECRETACCESSKEY")

		//Set Redis client (with or without ssl)
		jm := dbmgr.GetRedisClient(os.Getenv(Redis_Host))
		lcm.JobDataRedisClient = jm.Client
		// if lcm.JobDataRedisClient == nil {

		// 	if ok, _ := strconv.ParseBool(redisSSL); ok {
		// 		lcm.JobDataRedisClient = jm.Client

		// 		// lcm.JobDataRedisClient = goredis.NewClient(&goredis.Options{
		// 		// 	Addr:      os.Getenv(Redis_Host),
		// 		// 	Password:  os.Getenv(Redis_PWD),
		// 		// 	DB:        0,
		// 		// 	TLSConfig: &tls.Config{MinVersion: tls.VersionTLS12},
		// 		// })
		// 	} else {
		// 		lcm.JobDataRedisClient = dbmgr.GetRedisClient(lcm.YamlConfig).Client
		// 		// lcm.JobDataRedisClient = goredis.NewClient(&goredis.Options{
		// 		// 	Addr:     os.Getenv(Redis_Host),
		// 		// 	Password: os.Getenv(Redis_PWD),
		// 		// 	DB:       0,
		// 		// })
		// 	}
		//}

		//Get Env from Redis or Key Vault. If the value is not exist in Redis, read from Key Vault and cache into Redis

		//storageConn := getAzureKeyFromRedisOrKeyVault(storageAccount)
		//aws_access_key_id := getAzureKeyFromRedisOrKeyVault(awsKeyId)
		//aws_secret_access_key := getAzureKeyFromRedisOrKeyVault(awsSecretKeyId)

		gcs_service_account_key = getAzureKeyFromRedisOrKeyVault("gcp-service-account")

		decoded, err := base64.StdEncoding.DecodeString(gcs_service_account_key)
		if err != nil {
			glog.Fatalf("Failed to decode base64: %v", err)
		}

		// Step 3: Write to a temporary JSON file
		tmpDir := os.TempDir()
		tmpFile := filepath.Join(tmpDir, "gcp-creds.json")
		if err := os.WriteFile(tmpFile, decoded, 0600); err != nil {
			log.Fatalf("Failed to write temp file: %v", err)
		}
		//defer os.Remove(tmpFile)

		// Step 4: Set environment variable
		os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", tmpFile)

		// Set GCS credentials end

		eventhubconnStrFull = getAzureKeyFromRedisOrKeyVault("EventHubNameSpace--Full--FullyQualifiedNamespace")
		if u, err := url.Parse(eventhubconnStrFull); err == nil && u.Host != "" {
			eventhubconnStrFull = u.Host
		}

		eventhubNameFullTask = getAzureKeyFromRedisOrKeyVault("EventHubNameSpace--Full--EventHubName--Task")
		eventhubNameFullJob = getAzureKeyFromRedisOrKeyVault("EventHubNameSpace--Full--EventHubName--Job")

		eventhubconnStrDelta = getAzureKeyFromRedisOrKeyVault("EventHubNameSpace--Delta--FullyQualifiedNamespace")
		if u, err := url.Parse(eventhubconnStrDelta); err == nil && u.Host != "" {
			eventhubconnStrDelta = u.Host
		}

		eventhubNameUpsertTask = getAzureKeyFromRedisOrKeyVault("EventHubNameSpace--Delta--EventHubName--UpsertTask")
		eventhubNameUpsertJob = getAzureKeyFromRedisOrKeyVault("EventHubNameSpace--Delta--EventHubName--UpsertJob")

		eventhubNameDeleteTask = getAzureKeyFromRedisOrKeyVault("EventHubNameSpace--Delta--EventHubName--DeleteTask")
		eventhubNameDeleteJob = getAzureKeyFromRedisOrKeyVault("EventHubNameSpace--Delta--EventHubName--DeleteJob")
	}
	// lcm.AzTaskResultMgr = dbmgr.GetTaskEventHubClient(eventhubconnStrFull, eventhubNameFullTask)
	// lcm.AzJobResultMgr = dbmgr.GetJobEventHubClient(eventhubconnStrFull, eventhubNameFullJob)

	//Exit if any of these Envs are not exist.
	//if len(storageConn) == 0 || len(aws_access_key_id) == 0 || len(aws_secret_access_key) == 0 || len(cosmosdbConn) == 0 {
	//b2b if len(storageConn) == 0 || len(aws_access_key_id) == 0 || len(aws_secret_access_key) == 0 || len(eventhubconnStrFull) == 0 {
	//if len(storageConn) == 0 || len(eventhubconnStrFull) == 0 {
	if len(eventhubconnStrFull) == 0 {

		glog.Error("Cannot get keys (pkrange, storage conn, aws access key id, eventhub conn, aws secret access key) from key vault or redis")
		os.Exit(-1)
	}

	//Add Envs into system Env Value (Project Global Var)
	os.Setenv("AZCOPY_PARALLEL_STAT_FILES", "true")
	//b2b os.Setenv("AWS_ACCESS_KEY_ID", aws_access_key_id)
	//b2b os.Setenv("AWS_SECRET_ACCESS_KEY", aws_secret_access_key)
	//os.Setenv("AZ_BLOB_CONN_STRING", storageConn)
	//630
	os.Setenv(Full_EventHub_ConnStr, eventhubconnStrFull)
	os.Setenv(Full_EventHub_Task, eventhubNameFullTask)
	os.Setenv(Full_EventHub_Job, eventhubNameFullJob)

	os.Setenv(Delta_EventHub_ConnStr, eventhubconnStrDelta)
	os.Setenv(Delta_EventHub_UpsertTask, eventhubNameUpsertTask)
	os.Setenv(Delta_EventHub_UpsertJob, eventhubNameUpsertJob)

	os.Setenv(Delta_EventHub_DeleteTask, eventhubNameDeleteTask)
	os.Setenv(Delta_EventHub_DeleteJob, eventhubNameDeleteJob)

}
