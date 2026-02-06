package util

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path"
	"regexp"
	"strings"
	"time"

	"github.com/gogf/gf/os/glog"

	"github.com/Azure/azure-storage-azcopy/v10/config"
	"github.com/Azure/azure-storage-azcopy/v10/dbmgr"
)

const (
	TaskTypeFull = "Full"

	TaskTypeDeltaUpsert = "DeltaUpsert"

	TaskTypeDeltaDelete = "DeltaDelete"
)

func ParseSourceURL(sourceUrl string) (string, string, error) {
	var re = regexp.MustCompile(`https://(\w+)\.blob\.core\.windows\.net/([^/]+)/.*`)
	matches := re.FindStringSubmatch(sourceUrl)
	if len(matches) < 3 {
		return "", "", fmt.Errorf("invalid URL format: %s", sourceUrl)
	}
	sourceSaName := matches[1]
	containerName := matches[2]
	return sourceSaName, containerName, nil
}

func CalFileSize(files map[string]dbmgr.JobDataB2B) int64 {

	var totalSize int64 = 0
	for _, value := range files {
		totalSize += value.Size
	}

	return totalSize
}

// S3URLBuilder return a S3 URl.
func S3URLBuilder(data *dbmgr.AzJobResultData) string {
	//return fmt.Sprintf("https://%s.%s/%s", data.Bucket, config.S3_ENDPOINT, data.FileName)
	return fmt.Sprintf("https://%s", data.SourceURL)

}

// StringToTimeS3 parse string into S3 format time.
func StringToTimeS3(timeString string) time.Time {
	t, err := time.Parse("2006-01-02T15:04:05.000Z07:00", timeString)
	if err != nil {
		return time.Now()
	}
	return t.In(time.FixedZone("CST", 8*3600))
}

// GetPartitionKeyWithCurrentTime get a partition key with current time (to hours)
func GetPartitionKeyWithCurrentTime() string {
	return time.Now().Format("2006-01-02 15") + ":00"
}

func GetOperationTimeWithCurrentTime() string {
	return time.Now().Format(time.RFC3339)
}

// StringToTimeGMT parse string into GMT format time
func StringToTimeGMT(timeString string) (time.Time, error) {
	t, err := time.Parse(time.RFC1123, timeString)
	if err != nil {
		return time.Now(), err
	}
	return t.In(time.FixedZone("CST", 8*3600)), nil
}

// LocalS3FileFinder find all files in specified path
func LocalS3FileFinder(path string) ([]string, error) {
	files, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}
	var result []string
	for _, file := range files {
		result = append(result, file.Name())
	}
	return result, nil
}

// LocalS3FolderFinder find all folders in specified path
func LocalS3FolderFinder(path string) ([]string, error) {
	files, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}
	var result []string
	for _, file := range files {
		if file.IsDir() {
			result = append(result, file.Name())
		}
	}
	return result, nil
}

// ParseAzBlobConnString parse Azure blob connection string.
func ParseAzBlobConnString(connString string) map[string]string {
	valueSlice := strings.Split(connString, ";")
	result := make(map[string]string)
	for _, item := range valueSlice {
		itemSlice := strings.Split(item, "=")
		result[itemSlice[0]] = itemSlice[1]
	}
	result["AccountKey"] = result["AccountKey"] + "=="
	return result
}

// ParseContainerURL extracts the container URL from a full blob URL.
func ParseContainerURL(blobURL string) (string, error) {
	// Updated regex to match both URL formats and capture container name and blob path
	re := regexp.MustCompile(`https://(\w+)\.blob\.core\.windows\.net/([^/]+)(?:/(.*))?`)
	matches := re.FindStringSubmatch(blobURL)
	if len(matches) < 3 {
		return "", fmt.Errorf("invalid blob URL format: %s", blobURL)
	}
	containerName := matches[2]
	blobPath := matches[3]
	if blobPath == "" {
		blobPath = ""
	}
	return containerName + "/" + blobPath, nil
}

// MkdirWorkDirIfNotExist create a new download cache folder.
// Deprecated.
func MkdirWorkDirIfNotExist() {
	_, err := os.ReadDir(path.Join(config.LOCAL_FILE_SAVE_LOCATION_PREFIX))
	if err != nil {
		os.Mkdir(path.Join(config.LOCAL_FILE_SAVE_LOCATION_PREFIX), 0777)
	}
}

// MkdirWorkDirIfNotExistWin create a new download cache folder in windows.
// Deprecated.
func MkdirWorkDirIfNotExistWin() {
	_, err := os.ReadDir(config.LOCAL_FILE_SAVE_LOCATION_PREFIX_WIN)
	if err != nil {
		os.Mkdir(config.LOCAL_FILE_SAVE_LOCATION_PREFIX_WIN, 0777)
	}
}

// GenerateS3FileModifyTimeMapKey generate a key for map.
// Deprecated.
func GenerateS3FileModifyTimeMapKey(bucketName string, fileName string) string {
	return fmt.Sprintf("%s/%s", bucketName, fileName)
}

// DeleteLocalBucketWin delete local file cache
// Deprecated.
func DeleteLocalBucketWin(bucketName string) bool {
	err := os.RemoveAll(path.Join(config.LOCAL_FILE_SAVE_LOCATION_PREFIX, bucketName))
	if err != nil {
		return false
	}
	return true
}

// JobFailRateCalculator calculated the fail rate of a task.
func JobFailRateCalculator(totalJobs int, failRate float64) int {
	return int(math.Round(float64(totalJobs) * failRate))
}

// JobFailRateCalculator calculated the fail rate of a task.
func NewJobFailRateCalculator(totalJobs int) int {

	switch {
	case totalJobs < 10000:
		return int(math.Round(float64(totalJobs) * 0.001))

	default:
		return int(math.Round(float64(totalJobs) * 0.0001))
	}

}

// ArgsParser parse the "-t -j" args.
func ArgsParser(args []string) (string, string, string, string) {
	var taskId, jobId, overwriteOpts, logLevel string
	logLevel = "INFO"
	overwriteOpts = "true"
	for index, item := range args {
		if item == "-t" {
			taskId = args[index+1]
		}

		if item == "-j" {
			jobId = args[index+1]
		}
		if strings.HasPrefix(item, "--overwrite=") {
			//overwriteOpts = item
			overwriteOpts = strings.TrimPrefix(item, "--overwrite=")
		}
		if strings.HasPrefix(item, "--log-level=") {
			logLevel = strings.TrimPrefix(item, "--log-level=")
		}
	}
	return taskId, jobId, overwriteOpts, logLevel
}

// ParseAzureCosmosConn parse the Azure cosmos connection string.
func ParseAzureCosmosConn(conn string) (string, string, error) {
	host, pwd := "", ""
	if len(conn) == 0 {
		return host, pwd, errors.New("get cosmos db conn string failed")
	}
	kvs := strings.Split(conn, ";")
	for _, item := range kvs {
		if strings.HasPrefix(item, "AccountEndpoint=") {
			host = strings.TrimPrefix(item, "AccountEndpoint=")
		}

		if strings.HasPrefix(item, "AccountKey=") {
			pwd = strings.TrimPrefix(item, "AccountKey=")
		}

	}
	return host, pwd, nil
}

// ParseAzureRedisConn parse the Azure Redis connection string.
func ParseAzureRedisConn(conn string) (string, string, string, error) {
	host, pwd, ssl := "", "", ""
	if len(conn) == 0 {
		return host, pwd, ssl, errors.New("get redis conn string failed")
	}

	kvs := strings.Split(conn, ",")
	for _, item := range kvs {
		if strings.Contains(item, "=") {
			if strings.HasPrefix(item, "password=") {
				pwd = strings.TrimPrefix(item, "password=")
			}

			if strings.HasPrefix(item, "ssl=") {
				ssl = strings.TrimPrefix(item, "ssl=")
			}

		} else {
			host = item
		}
	}
	return host, pwd, ssl, nil
}

// GetAzureRedisCredential decrypt Redis connection string.
func GetAzureRedisCredential(redisEnv string, jobId string) (string, string, string, error) {
	redisInfo := os.Getenv(redisEnv)
	if len(redisInfo) == 0 {
		glog.Error("can't get redis conn from env")
		os.Exit(-1)
	}

	redisConn := redisInfo
	if len(os.Getenv("DEBUG_MODE")) == 0 {

		//redisConn = AesDecrypt(redisInfo, jobId)
	}

	if len(redisConn) == 0 {
		glog.Error("can't get redis conn from decrypt")
		os.Exit(-1)
	}
	return ParseAzureRedisConn(redisConn)
}
func AesDecrypt(cryted string, key string) string {
	// 转成字节数组
	crytedByte, _ := base64.StdEncoding.DecodeString(cryted)
	k := []byte(key)
	orig := AesDecryptCBC(crytedByte, k)
	// if err != nil {
	// 	return ""
	// }
	//orig = PKCS7UnPadding(orig)
	return string(orig)
}
func AesEncrypt(cryted string, key string) string {
	// 转成字节数组
	crytedByte, _ := base64.StdEncoding.DecodeString(cryted)
	k := []byte(key)
	orig := AesEncryptCFB(crytedByte, k)
	// if err != nil {
	// 	return ""
	// }
	//orig = PKCS7UnPadding(orig)
	return string(orig)
}
func AesEncryptCFB(origData []byte, key []byte) (encrypted []byte) {
	block, err := aes.NewCipher(key)
	if err != nil {
		panic(err)
	}
	encrypted = make([]byte, aes.BlockSize+len(origData))
	iv := encrypted[:aes.BlockSize]
	if _, err := io.ReadFull(rand.Reader, iv); err != nil {
		panic(err)
	}
	stream := cipher.NewCFBEncrypter(block, iv)
	stream.XORKeyStream(encrypted[aes.BlockSize:], origData)
	return encrypted
}
func AesDecryptCFB(encrypted []byte, key []byte) []byte {
	block, _ := aes.NewCipher(key)
	if len(encrypted) < aes.BlockSize {
		panic("ciphertext too short")
	}
	iv := encrypted[:aes.BlockSize]
	encrypted = encrypted[aes.BlockSize:]

	stream := cipher.NewCFBDecrypter(block, iv)
	stream.XORKeyStream(encrypted, encrypted)
	return encrypted

}
func AesDecryptCBC(crypted []byte, key []byte) (decrypted []byte) {
	block, _ := aes.NewCipher(key)
	blockSize := block.BlockSize()
	blockMode := cipher.NewCBCDecrypter(block, key[:blockSize])
	origData := make([]byte, len(crypted))
	blockMode.CryptBlocks(origData, crypted)
	origData = PKCS7UnPadding(origData)
	return origData[blockSize:]
}

// 补码
// AES加密数据块分组长度必须为128bit(byte[16])，密钥长度可以是128bit(byte[16])、192bit(byte[24])、256bit(byte[32])中的任意一个。
func PKCS7Padding(ciphertext []byte, blocksize int) []byte {
	padding := blocksize - len(ciphertext)%blocksize
	padtext := bytes.Repeat([]byte{byte(padding)}, padding)
	return append(ciphertext, padtext...)
}

// 去码
func PKCS7UnPadding(origData []byte) []byte {
	length := len(origData)
	unpadding := int(origData[length-1])
	return origData[:(length - unpadding)]
}

//func AzblobURLBuilder(accountName string, containerName string, bucketName string, fileName string, sas string) string {
//	return fmt.Sprintf("https://%s.blob.core.windows.net/%s/%s/%s?%s", accountName, containerName, bucketName, fileName, sas)
//}

// IsUploadToAzBlob return true when S3 object is able to upload
//
//	func IsUploadToAzBlob(container string, fileName string, s3LastModifiedTime time.Time) bool {
//		requestURL := config.AZ_BLOB_ENDPOINT + container + "/" + fileName + "?comp=metadata&" + config.AZ_BLOB_SAS
//		resp, err := http.Get(requestURL)
//		if err != nil {
//			return true
//		}
//		azBlobLastModifiedTime := resp.Header.Get("Last-Modified")
//		if azBlobLastModifiedTime == "" {
//			return true
//		} else {
//			azBlobLastModifiedTime, err := StringToTimeGMT(azBlobLastModifiedTime)
//			if err != nil {
//				return false
//			}
//			if IsTimeALateB(azBlobLastModifiedTime, s3LastModifiedTime) {
//				return false
//			}
//		}
//		return true
//	}
var fileNameRegex = regexp.MustCompile(`^(DeltaDelete|DeltaUpsert|Full)-\d+-[a-z0-9]+`)

func validateAndExtractTaskType(fileName string) (bool, string) {

	matches := fileNameRegex.FindStringSubmatch(fileName)
	if matches == nil {
		return false, ""
	}
	return true, matches[1]
}

// func main() {
// 	 path := "./json_files"
// 	 // Path to the JSON files directory
// 	 if err := validateFileNames(path); err != nil {
// 		fmt.Println("Error reading directory:", err)
// 		os.Exit(1)
// 		}
// 		}

// 620 Get TaskType from FileName.
func GetTaskTypeFromFileName(fileName string) (bool, string) {

	// taskType := ""
	// if len(fileName) == 0 {
	// 	return taskType, errors.New("get TaskType from  FileName failed")
	// }

	// isValid, taskType := validateAndExtractTaskType(fileName)

	// if !isValid {

	// 	return taskType, errors.New("file name is not valid")
	// }
	// if !validateFileNames(fileName) {
	// 	return taskType, errors.New("validate File Name failed")
	// }

	// kvs := strings.Split(fileName, "-")
	// for _, item := range kvs {
	// 	if strings.HasPrefix(item, TaskTypeFull) {
	// 		taskType = TaskTypeFull
	// 	}

	// 	if strings.HasPrefix(item, TaskTypeDeltaUpsert) {
	// 		taskType = TaskTypeDeltaUpsert
	// 	}

	// 	if strings.HasPrefix(item, TaskTypeDeltaDelete) {
	// 		taskType = TaskTypeDeltaDelete
	// 	}

	// }
	return validateAndExtractTaskType(fileName)
}
