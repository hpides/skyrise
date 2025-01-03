#pragma once

#include "utils/literal.hpp"

namespace skyrise {

const std::string kConstPrefix = "k";

const std::string kBaseTag = "Skyrise";
const std::string kBenchmarkTag = "SkyriseBenchmark";
const std::string kCoordinatorTag = "SkyriseCoordinator";
const std::string kWorkerTag = "SkyriseWorker";

/**
 * The maximum item size in DynamoDB.
 */
inline constexpr size_t kDynamoDbMaxItemSizeBytes = 400_KB;

/**
 *  Hard limits in DynamoDB for batch requests.
 *  Exceeding them will either cause a ValidationException or the entire batch operations is rejected.
 */
inline constexpr uint kDynamoDbBatchGetItemLimit = 100;
inline constexpr uint kDynamoDbBatchWriteItemLimit = 25;

/**
 * The AWS Lambda service has a burst concurrency quota which limits the number of function instances serving requests
 * at a given time. The cumulative function concurrency in a region can reach an initial level of between 500 and 3000,
 * which varies per Region (cf. https://docs.aws.amazon.com/lambda/latest/dg/invocation-scaling.html)
 */
inline constexpr size_t kLambdaFunctionConcurrencyLimit = 3000;

/**
 * The name of the method that AWS Lambda calls to execute the function.
 */
inline constexpr std::string_view kLambdaFunctionHandler = "FunctionHandler";

/**
 * The name of the AWS IAM role for the Lambda function.
 */
inline constexpr std::string_view kLambdaFunctionRoleName = "AWSLambda";

/**
 * The time between repetitions.
 * A 5 second sleep currently ensures that the network token bucket is completely refilled. However, this depends on the
 * token bucket configuration.
 */
inline constexpr uint16_t kLambdaNetworkTokenBucketRefillSeconds = 5;

/**
 *  At 1,769 MB, a function has the equivalent of one vCPU (one vCPU-second of credits per second).
 */
inline constexpr uint16_t kLambdaVcpuEquivalentMemorySizeMb = 1769;

inline constexpr uint16_t kLambdaMaximumMemorySizeMb = 10240;

/**
 * The size of the footer in Parquet files.
 */
inline constexpr uint32_t kParquetFooterSizeBytes = 64_KB;

/**
 * The size of file metadata in the Parquet footer in bytes.
 */
inline constexpr uint32_t kParquetFooterMetadataSizeBytes = 4;

/**
 * The offset of file metadata size in the Parquet footer in bytes.
 */
inline constexpr uint32_t kParquetFooterMetadataOffsetBytes = 8;

/**
 * Coordinator binary and function names.
 */
inline const std::string kCoordinatorBinaryName = "skyriseCoordinatorFunction";
inline const std::string kCoordinatorFunctionName = kCoordinatorBinaryName;

/**
 * Worker binary and function names.
 */
inline const std::string kWorkerBinaryName = "skyriseWorkerFunction";
inline const std::string kWorkerFunctionName = kWorkerBinaryName;

/**
 * Response queue name prefix.
 */
inline const std::string kRequestResponseQueueNamePrefix = "skyriseResponseQueue-";

/**
 * Function request parameter.
 */
inline const std::string kRequestCompressedAttribute = "request_compressed";
inline const std::string kRequestDecompressedSizeAttribute = "decompressed_size";
inline const std::string kRequestEnableIntrospection = "enable_introspection";
inline const std::string kRequestEnableMetering = "enable_metering";
inline const std::string kRequestResponseQueueUrlAttribute = "response_queue_url";

/**
 * Function response parameter.
 */
inline const std::string kResponseIsSuccessAttribute = "isSuccess";
inline const std::string kResponseMessageAttribute = "message";
inline const std::string kResponseMeteringAttribute = "metering";
inline const std::string kResponseMeteringFailedAttribute = "failed";
inline const std::string kResponseMeteringFinishedAttribute = "finished";
inline const std::string kResponseMeteringRetriedAttribute = "retried";
inline const std::string kResponseMeteringSucceededAttribute = "succeeded";

/**
 * Coordinator request and response parameters.
 */
inline const std::string kCoordinatorRequestCompilerNameAttribute = "compiler_name";
inline const std::string kCoordinatorRequestQueryPlanAttribute = "query_plan";
inline const std::string kCoordinatorRequestQueryStringAttribute = "query_string";
inline const std::string kCoordinatorRequestScaleFactorAttribute = "scale_factor";
inline const std::string kCoordinatorRequestStoragePrefixAttribute = "storage_prefix";
inline const std::string kCoordinatorRequestWorkerFunctionAttribute = "worker_function_name";
inline const std::string kCoordinatorResponseResultObjectAttribute = "result";
inline const std::string kCoordinatorResponseExecutionStatisticsAttribute = "execution_statistics";
inline const std::string kCoordinatorResponseExecutionRuntimeAttribute = "execution_runtime_ms";
inline const std::string kCoordinatorResponseMemorySizeAttribute = "coordinator_memory_size_mb";
inline const std::string kCoordinatorResponseWorkerMemorySizeAttribute = "worker_memory_size_mb";
inline const std::string kCoordinatorResponseWorkerInvocationCountAttribute = "worker_invocation_count";
inline const std::string kCoordinatorResponseWorkerAccumulatedRuntimeAttribute = "worker_accumulated_runtime_ms";

/**
 * Worker request and response parameters.
 */
inline const std::string kWorkerRequestIdAttribute = "worker_id";
inline const std::string kWorkerRequestPqpPipelineTemplateAttribute = "pqp_pipeline_template";
inline const std::string kWorkerRequestFragmentDefinitionsAttribute = "fragment_definitions";
inline const std::string kWorkerRequestFragmentDefinitionAttribute = "fragment_definition";
inline const std::string kWorkerResponseIdAttribute = kWorkerRequestIdAttribute;
inline const std::string kWorkerResponseMemorySizeMbAttribute = "worker_memory_size_mb";
inline const std::string kWorkerResponseRuntimeMsAttribute = "worker_runtime_ms";
inline const std::string kWorkerResponseImportDataSizeBytesAttribute = "import_data_size_bytes";
inline const std::string kWorkerResponseExportDataSizeBytesAttribute = "export_data_size_bytes";

/**
 * S3 bucket for benchmark datasets.
 */
inline const std::string kS3BenchmarkDatasetsBucket = "benchmark-data-sets";

/**
 * Storage containers for benchmarks.
 */
inline const std::string kSkyriseBenchmarkContainer = "skyrise-benchmark";
inline const std::string kS3ExpressBucket = "skyrise-benchmark--use1-az4--x-s3";

/**
 * S3 bucket for CI tests.
 */
inline const std::string kSkyriseTestBucket = "skyrise-ci";

}  // namespace skyrise
