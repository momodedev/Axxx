package config

// Debug mode
const (
	DEBUG_MODE = "DEBUG_MODE"
)

// Azcopy System Env
const (
	AZCOPY_CONCURRENCY_VALUE = "AZCOPY_CONCURRENCY_VALUE"
)

// Azure Blob SAS
const (
	AZ_BLOB_CONTAINER   = "AZ_BLOB_CONTAINER"
	AZ_BLOB_CONN_STRING = "AZ_BLOB_CONN_STRING"
)

// S3 Credentials
const (
	S3_ENDPOINT    = "s3.us-west-2.amazonaws.com"
	AWS_KEY_ID     = "AWS_ACCESS_KEY_ID"
	AWS_ACCESS_KEY = "AWS_SECRET_ACCESS_KEY"
)

// Locker File Save Location
const (
	LOCAL_FILE_SAVE_LOCATION_PREFIX     = "goS3CopyCache"
	LOCAL_FILE_SAVE_LOCATION_PREFIX_WIN = ".\\goS3copyCache\\"
)

// Multiple Thread Limiter
//const (
//	LIMITER_FROM_S3_TO_LOCAL = 5
//	LIMITER_FROM_LOCAL_TO_AZ = 5
//	LIMITER_FROM_S3_TO_AZ    = 5
//)

// CosmosDB
//const (
//	COSMOS_ENDPOINT       = "https://s32adls-dev.documents.azure.com:443/"
//	COSMOS_KEY            = ""
//	COSMOS_DB             = "DataMigration"
//	COSMOS_CONTAINER_JOB  = "MigrationTasks"
//	COSMOS_CONTAINER_TASK = "orctask"
//)

// Redis Connection string
//const (
//	REDIS_HOST = "S32ADLS-Dev.redis.cache.windows.net:6380"
//	REDIS_PSW  = ""
//)
