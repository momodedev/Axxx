package dbmgr

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-storage-azcopy/v10/config"
	"github.com/avast/retry-go"
	"github.com/gogf/gf/os/glog"
	goredis "github.com/redis/go-redis/v9"
)

type JobMgr struct {
	//Client redigo.Conn
	Client      *goredis.Client
	CopyJobsS3  []JobDataS3
	CopyJobsB2B []JobDataB2B
}

type JobDataS3 struct {
	BucketName    string `json:"BucketName"`
	Region        string `json:"Region"`
	FilePath      string `json:"FilePath"`
	SourceUrl     string `json:"SourceUrl"`
	Size          int64  `json:"Size"`
	VersionId     string `json:"VersionId"`
	ModifyTime    string `json:"ModifyTime"`
	Etag          string `json:"Etag"`
	StorageClass  string `json:"StorageClass"`
	Accessier     string `json:"Accessier"`
	ContainerName string `json:"ContainerName"`
}

type JobDataB2B struct {
	//BucketName   string `json:"BucketName"`
	//Region       string `json:"Region"`
	FileName     string `json:"FileName"`
	FilePath     string `json:"FilePath"`
	DestFilePath string `json:"DestFilePath"`
	SourceUrl    string `json:"SourceUrl"`
	Size         int64  `json:"Size"`
	//VersionId    string `json:"VersionId"`
	ModifyTime string `json:"ModifyTime"`
	//Etag         string `json:"Etag"`
	//StorageClass string `json:"StorageClass"`
	SrcTier       string `json:"SrcTier"`
	ContainerName string `json:"ContainerName"`
	DestTier      string `json:"DestTier"`
}

func GetRedisClient(redishost string) *JobMgr {
	glog.Debugf("GetRedisClient %s", redishost)
	credential, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		// handle error
		glog.Error("failed to obtain a credential")
		os.Exit(-1)
	}
	rdb := goredis.NewClient(&goredis.Options{
		Addr:                       redishost,
		CredentialsProviderContext: redisCredentialProviders(credential),
		TLSConfig:                  &tls.Config{MinVersion: tls.VersionTLS12},
	})
	// use the client
	return &JobMgr{Client: rdb}
}

func redisCredentialProviders(credential azcore.TokenCredential) func(context.Context) (string, string, error) {
	return func(ctx context.Context) (string, string, error) {
		glog.Debugf("getting access token")
		// get an access token for Azure Cache for Redis
		tk, err := credential.GetToken(ctx, policy.TokenRequestOptions{
			// Azure Cache for Redis uses the same scope in all clouds
			Scopes: []string{"https://redis.azure.com/.default"},
		})
		if err != nil {
			glog.Errorf("failed to get token: %s", err)
			//return "", "", err
		}
		// the token is a JWT; get the principal's object ID from its payload
		parts := strings.Split(tk.Token, ".")
		if len(parts) != 3 {
			glog.Error("token must be a JWT")
			//return "", "", errors.New("token must have 3 parts")
		}
		payload, err := base64.RawURLEncoding.DecodeString(parts[1])
		if err != nil {
			glog.Error("couldn't base64 decode payload")
			//return "", "", fmt.Errorf("couldn't decode payload: %s", err)
		}
		claims := struct {
			OID string `json:"oid"`
		}{}
		err = json.Unmarshal(payload, &claims)
		if err != nil {
			glog.Error("couldn't unmarshal payload")
			//return "", "", fmt.Errorf("couldn't unmarshal payload: %s", err)
		}
		if claims.OID == "" {
			glog.Error("missing object ID claim")
			//return "", "", errors.New("missing object ID claim")
		}
		return claims.OID, tk.Token, nil
	}
}

// GetRedisClient return Redis client (go-redis)
func GetRedisClientOld(goS3Config *config.Config) *JobMgr {
	rdb := goredis.NewClient(&goredis.Options{
		Addr:      goS3Config.REDIS_HOST,
		Password:  goS3Config.REDIS_PSW,
		DB:        0,
		TLSConfig: &tls.Config{MinVersion: tls.VersionTLS12},
	})

	return &JobMgr{Client: rdb}
}

// GetJobData retrieve incremental job data from redis
func (r *JobMgr) GetJobData(key string, ctx context.Context) error {
	newJob := new([]JobDataB2B)
	jobs, err := r.Client.Get(ctx, key).Result()
	if err != nil {
		return err
	}

	err = json.Unmarshal([]byte(jobs), &newJob)
	if err != nil {
		return err
	}

	r.CopyJobsB2B = *newJob

	return nil
}

// DeleteJobData delete job data in Redis with key.
func (r *JobMgr) DeleteJobData(key string, ctx context.Context) error {
	return r.Client.Del(ctx, key).Err()
}

// BandwidthBoosterByFileSize resize the CopyJobs slice with unstable sort DESC.
func (r *JobMgr) BandwidthBoosterByFileSize() {
	sort.Slice(r.CopyJobsB2B, func(i, j int) bool {
		// if r.CopyJobsB2B[i].Size < r.CopyJobsB2B[j].Size {
		// 	return true
		// }
		// return false
		return r.CopyJobsB2B[i].Size > r.CopyJobsB2B[j].Size
	})
}

// GetRedisKey get Redis value with key, retry 3 times if failed (1s interval).
func GetRedisKey(key string, client *goredis.Client) string {
	var val string
	var err error
	retry.Do(func() error {
		val, err = client.Get(context.TODO(), key).Result()
		if err != nil {
			//fmt.Printf("can't get key %s from redis", key)
			return err
		}
		return nil
	}, retry.Attempts(3), retry.Delay(1000*time.Microsecond))

	if err != nil {
		glog.Errorf("can't get key %s from redis", key)

	}

	return val
}

//func GetRedigoClient() *JobMgr {
//	conn, err := redigo.Dial(
//		"tcp",
//		config.REDIS_HOST,
//		redigo.DialPassword(config.REDIS_PSW),
//		redigo.DialDatabase(0),
//		redigo.DialTLSConfig(&tls.Config{MinVersion: tls.VersionTLS12}))
//	if err != nil {
//		glog.Error("Err during connect to redis: " + err.Error())
//	}
//	return &JobMgr{Client: conn}
//}
