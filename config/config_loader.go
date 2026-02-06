package config

import (
	"os"

	"github.com/gogf/gf/os/glog"
	"gopkg.in/yaml.v3"
)

type Config struct {
	REDIS_HOST string
	REDIS_PSW  string

	// COSMOS_ENDPOINT       string `yaml:"COSMOS_ENDPOINT"`
	// COSMOS_KEY            string `yaml:"COSMOS_KEY"`
	// COSMOS_DB             string `yaml:"COSMOS_DB"`
	// COSMOS_CONTAINER_JOB  string `yaml:"COSMOS_CONTAINER_JOB"`
	// COSMOS_CONTAINER_TASK string `yaml:"COSMOS_CONTAINER_TASK"`

	//620
	EVENTHUB_CONN_STR  string `yaml:"EVENTHUB_CONN_STR"`
	EVENTHUB_NAME_TASK string `yaml:"EVENTHUB_NAME_TASK"`
	EVENTHUB_NAME_JOB  string `yaml:"EVENTHUB_NAME_JOB"`

	LOCAL_WORK_MODE int `yaml:"LOCAL_WORK_MODE"`

	LIMITER_FROM_S3_TO_LOCAL int `yaml:"LIMITER_FROM_S3_TO_LOCAL"`
	LIMITER_FROM_LOCAL_TO_AZ int `yaml:"LIMITER_FROM_LOCAL_TO_AZ"`
	LIMITER_FROM_S3_TO_AZ    int `yaml:"LIMITER_FROM_S3_TO_AZ"`

	DEBUG_MODE int

	AZCOPY_CONCURRENCY_VALUE int    `yaml:"AZCOPY_CONCURRENCY_VALUE"`
	AZ_KEYVAULT              string `yaml:"AZ_KEYVAULT"`
}

func LoadYaml() (*Config, error) {
	glog.Info("Loading Configurations ...")
	yamlFile, err := os.ReadFile("config.yaml")
	if err != nil {
		glog.Errorf("Fail Loading YAML file config.yaml, Error is:%s", err)
		return nil, err
	}

	goS3Config := new(Config)
	err = yaml.Unmarshal(yamlFile, goS3Config)
	if err != nil {
		glog.Errorf("Fail Loading YAML file config.yaml, Error is:%s", err)
		return nil, err
	}

	return goS3Config, nil
}
