#pragma once

#include "constants.hpp"
#include "types.hpp"
#include "utils/literal.hpp"

namespace skyrise {

inline constexpr size_t kEc2RequestIntervalMilliseconds = 100;

inline constexpr std::string_view kEfsArn =
    "arn:aws:elasticfilesystem:us-east-1:613468429976:access-point/fsap-02a7873d469bbc470";

inline constexpr std::string_view kEfsMountPoint = "/mnt/efs/";

/**
 * Enables the compression of worker invocation payloads.
 */
inline constexpr bool kLambdaFunctionInvocationPayloadCompression = true;

/**
 * After creation, an AWS Lambda function is initially in a pending state until all required resources are available
 * (cf. https://docs.aws.amazon.com/lambda/latest/dg/functions-states.html). We wait and poll for the function's state
 * to become active before we start invoking the function. Based on our measurements, this process is not supposed to
 * take longer than 10 seconds without VPC configuration and 5 minutes (=300 seconds) with VPC configuration.
 */
inline constexpr size_t kLambdaFunctionStatePollingTimeoutSeconds = 300;

/**
 * The AWS Lambda service-side timeout limit for functions is 15 minutes (cf.
 * https://docs.aws.amazon.com/lambda/latest/dg/gettingstarted-limits.html). Our own timeout is reduced to 5 minutes,
 * because we expect our cloud functions to never run longer than that. Thereby, we keep costs for erroneous function
 * invocations that never terminate in check.
 */
inline constexpr size_t kLambdaFunctionTimeoutSeconds = 300;

/**
 * The interval for polling a service's state.
 */
inline constexpr size_t kStatePollingIntervalMilliseconds = 100;

/**
 * The timeout for polling a service's state.
 */
inline constexpr size_t kStatePollingTimeoutSeconds = 300;

/**
 * The default database schema name in Glue.
 */
inline constexpr std::string_view kDatabaseSchemaName = "SkyriseDB";

/**
 * The default bucket name and prefix for storing query results.
 */
inline constexpr std::string_view kExportBucketName = "skyrise";
inline constexpr std::string_view kExportRootPrefix = "result/";

/**
 * The default export format for storing query results.
 */
inline constexpr FileFormat kFinalResultsExportFormat = FileFormat::kCsv;
inline constexpr FileFormat kIntermediateResultsExportFormat = FileFormat::kParquet;

/**
 * Defines the per item size the DynamoDB storage reserves for metadata (e.g., timestamp or checksum).
 */
inline constexpr size_t kDynamoDbStorageMetadataSizeBytes = 1_KB;

/**
 * The maximum size for files to be read from the local filesystem.
 */
inline constexpr size_t kMaxFileSizeBytes = 2_GB;

/**
 * When asynchronously reading objects, we prebuild the requests based on the columns that are accessed, i.e.,
 * we perform projection push down. For that, we utilize the kReadRequestPaddingSize to find gaps in the file that
 * determine the borders of the requests.
 */
inline constexpr uint32_t kReadRequestPaddingSizeBytes = 1_MB;

/**
 * AWS advises to issue requests in the range of 8 MB - 16MB (c.f.
 * https://docs.aws.amazon.com/AmazonS3/latest/userguide/optimizing-performance-guidelines.html)
 */
inline constexpr uint32_t kS3ReadRequestSizeBytes = 16_MB;

/**
 * A timeout of 500-2,000 ms is sufficient for requests with a size of 8-16 MB. This requires that the I/O thread pool
 * is aligned with the available vCPUs. Otherwise, we will encounter many retries as a result of reduced performance of
 * individual requests.
 * Warning: The duration is based on empirical evaluation and should be changed with caution.
 */
static constexpr size_t kS3RequestTimeoutMs = 2000;

/**
 * The I/O thread pool for asynchronous requests must be aligned with the available vCPUs. This improves the stability
 * for multiple requests that are issued concurrently and enables to detect and re-trigger straggling requests.
 * Warning: The ratio is based on empirical evaluation and should be changed with caution.
 */
static constexpr double kIoThreadPoolToCpuRatio = 2.7;

/**
 * The lifetime after which EC2 instances are forced to terminate.
 */
static constexpr uint16_t kEc2TerminationGuardMinutes = 30;

/**
 * The maximum number of workers for the execution of a PQP pipeline.
 */
inline constexpr size_t kWorkerMaximumCountPerPipeline = kLambdaFunctionConcurrencyLimit;

/**
 * The number of worker invocations at which we switch to recursive invocation.
 */
inline constexpr size_t kWorkerRecursiveInvocationThreshold = 32;

/**
 * The frequency in milliseconds at which we poll for the approximate SQS queue size.
 */
inline constexpr int kSqsApproximateQueueSizePollIntervalMilliseconds = 100;

/**
 * The range of IP addresses (subnet) of the default VPC.
 */
const Aws::String kDefaultSubnetId = "subnet-1225ae5f";

/**
 * The availability zone we commonly use in the benchmarks.
 */
const Aws::String kDefaultAvailabilityZone = "us-east-1a";
const Aws::String kDefaultSubnet = "subnet-1225ae5f";
const Aws::String kDefaultSecurityGroup = "sg-4a242c6a";
const Aws::String kSshSecurityGroup = "sg-05fb6181d99306e8c";

}  // namespace skyrise
