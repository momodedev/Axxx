package dbmgr

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/gogf/gf/os/glog"
)

func TestGetClient(t *testing.T) {
	ctx := context.Background()
	//goS3config, _ := config.LoadYaml()
	rdb := GetRedisClient("goS3config")

	val, err := rdb.Client.Get(ctx, "e3012e10-5057-4b2f-822b-d52054e8f504").Result()
	if err != nil {
		panic(err)
	}
	fmt.Println("TestKey", val)
}

//func TestGetRedigoClient(t *testing.T) {
//	rdb := GetRedigoClient()
//	do, err := rdb.Client.Do("GET", "84d45a15-ec0b-4236-90af-7bc589b5e9c2")
//	if err != nil {
//		t.Error("Cannot get from redis")
//	}
//
//	glog.Info(do)
//}

func TestGoS3RedisClient_GetIncrementalJobData(t *testing.T) {
	ctx := context.Background()
	//goS3config, _ := config.LoadYaml()
	rdb := GetRedisClient("goS3config")

	err := rdb.GetJobData("20221203062930604-41dabeebf5f741e39903ba12887daca7", ctx)
	if err != nil {
		t.Error(err)
	} else {
		glog.Infof("%+v", rdb.CopyJobs)
	}
}

func TestJobMgr_BandwidthBoosterByFileSize(t *testing.T) {
	jobs := &JobMgr{
		Client:   nil,
		CopyJobs: []JobData{},
	}

	for i := 1; i <= 10; i++ {
		job := JobData{
			BucketName:   "lwazcopy",
			Region:       "us-west-2",
			FilePath:     fmt.Sprintf("Img%d.png", i),
			SourceUrl:    "",
			Size:         int64(rand.Int()),
			VersionId:    "ThisIsVerSion",
			ModifyTime:   "2022-12-01T17:34:40Z",
			Etag:         "",
			StorageClass: "666",
		}
		jobs.CopyJobs = append(jobs.CopyJobs, job)
		//time.Sleep(1 * time.Second)
	}
	jobs.BandwidthBoosterByFileSize()
	glog.Info(jobs)
}

func TestJobMgr_DeleteJobData(t *testing.T) {
	ctx := context.Background()
	//goS3config, _ := config.LoadYaml()
	rdb := GetRedisClient("goS3config")

	rdb.Client.Set(ctx, "TTT", "ThisisForTesting", 0)
	data, err := rdb.Client.Get(ctx, "TTT").Result()
	if err != nil {
		t.Error(err.Error())
	}

	glog.Info(data)

	err = rdb.DeleteJobData("TTT", ctx)
	if err != nil {
		t.Error(err.Error())
	}

	_, err = rdb.Client.Get(ctx, "TTT").Result()
	if err != nil {
		t.Error(err.Error())
	}

}
