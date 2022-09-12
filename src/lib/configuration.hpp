#pragma once

#include "types.hpp"
#include "utils/literal.hpp"

namespace skyrise {

/**
 * The name of the method that AWS Lambda calls to execute the function.
 */
inline constexpr std::string_view kLambdaFunctionHandler = "FunctionHandler";

/**
 * The name of the AWS IAM role for the Lambda function.
 */
inline constexpr std::string_view kLambdaFunctionRoleName = "AWSLambda";

/**
 * After creation, an AWS Lambda function is initially in a pending state until all required resources are available
 * (cf. https://docs.aws.amazon.com/lambda/latest/dg/functions-states.html). We wait and poll for the function's state
 * to become active before we start invoking the function. Based on our measurements, this process is not supposed to
 * take longer than 10 seconds.
 */
inline constexpr size_t kLambdaFunctionStatePollingTimeoutSeconds = 10;

/**
 * The AWS Lambda service-side timeout limit for functions is 15 minutes (cf.
 * https://docs.aws.amazon.com/lambda/latest/dg/gettingstarted-limits.html). Our own timeout is reduced to 5 minutes,
 * because we expect our cloud functions to never run longer than that. Thereby, we keep costs for erroneous function
 * invocations that never terminate in check.
 */
inline constexpr size_t kLambdaFunctionTimeoutSeconds = 300;

/**
 * The AWS Lambda service has a burst concurrency quota which limits the number of function instances serving requests
 * at a given time. The cumulative function concurrency in a region can reach an initial level of between 500 and 3000,
 * which varies per Region (cf. https://docs.aws.amazon.com/lambda/latest/dg/invocation-scaling.html)
 */
inline constexpr size_t kLambdaFunctionConcurrencyLimit = 3000;

/**
 * The interval length for polling a service's state.
 */
inline constexpr size_t kStatePollingIntervalMilliseconds = 100;

/**
 * The maximum size for files to be read from the local filesystem.
 */
inline constexpr size_t kMaxFileSize = 2_GB;

/**
 * The default bucket name and prefix for storing query results.
 */
inline constexpr std::string_view kExportBucketName = "skyrise";
inline constexpr std::string_view kExportRootPrefix = "result/";

/**
 * The default export format for storing query results.
 */
inline constexpr ExportFormat kFinalResultsExportFormat = ExportFormat::kCsv;
inline constexpr ExportFormat kIntermediateResultsExportFormat = ExportFormat::kOrc;

/**
 * The typical size for byte-range requests on S3 storage.
 */
inline constexpr size_t kS3NaturalReadSize = 20_MB;

/**
 * Defines the maximum number of workers for the execution of a PqpPipeline.
 */
inline constexpr size_t kMaxWorkerCountPerPipeline = kLambdaFunctionConcurrencyLimit;

/**
 * The default database schema name in Glue.
 */
inline constexpr std::string_view kDatabaseSchemaName = "SkyriseDB";

/**
 * Defines the per item size the DynamoDB storage reserves for metadata (e.g., timestamp or checksum).
 */
inline constexpr size_t kDynamoDbStorageMetadataSize = 1_KB;

}  // namespace skyrise
