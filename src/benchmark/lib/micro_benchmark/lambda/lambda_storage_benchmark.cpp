#include "lambda_storage_benchmark.hpp"

#include <numeric>

#include <aws/sqs/model/CreateQueueRequest.h>
#include <aws/sqs/model/DeleteQueueRequest.h>
#include <aws/sqs/model/GetQueueAttributesRequest.h>
#include <aws/sqs/model/PurgeQueueRequest.h>
#include <magic_enum/magic_enum.hpp>

#include "benchmark_result_aggregate.hpp"
#include "storage/backend/s3_utils.hpp"
#include "utils/assert.hpp"
#include "utils/string.hpp"
#include "utils/unit_conversion.hpp"

namespace skyrise {

namespace {

const Aws::String kName = "lambdaStorageBenchmark";
const Aws::String kFunctionName = "skyriseStorageIoFunction";
const std::string kContainer = kSkyriseBenchmarkContainer;
const std::string kObjectPrefix = GetUniqueName(kName) + "/data";
const std::string kSharedObjectPrefix = kName + "/data";
const auto kExpectedInvocationResponseTemplate =
    Aws::Utils::Json::JsonValue()
        // TODO(tobodner): Add validation for doubles.
        .WithString("invocation_start_timestamp", "0")
        .WithString("measurement_start_timestamp", "0")
        .WithString("measurement_end_timestamp", "0")
        .WithArray("request_latencies_ms", Aws::Utils::Array<Aws::Utils::Json::JsonValue>(0));

}  // namespace

LambdaStorageBenchmark::LambdaStorageBenchmark(
    std::shared_ptr<const CostCalculator> cost_calculator,
    std::shared_ptr<const Aws::DynamoDB::DynamoDBClient> dynamodb_client,
    std::shared_ptr<const Aws::EFS::EFSClient> efs_client, std::shared_ptr<const Aws::S3::S3Client> s3_client,
    std::shared_ptr<const Aws::SQS::SQSClient> sqs_client, const std::vector<size_t>& function_instance_sizes_mb,
    const std::vector<size_t>& concurrent_instance_counts, const size_t repetition_count,
    const std::vector<size_t>& after_repetition_delays_min, const size_t object_size_kb, const size_t object_count,
    const bool enable_writes, const bool enable_reads, const std::vector<StorageSystem>& storage_types,
    const bool enable_s3_eoz, const bool enable_vpc)
    : LambdaBenchmark(std::move(cost_calculator)),
      dynamodb_client_(std::move(dynamodb_client)),
      efs_client_(std::move(efs_client)),
      s3_client_(std::move(s3_client)),
      sqs_client_(std::move(sqs_client)) {
  std::vector<StorageOperation> operation_types{};
  if (enable_writes) {
    operation_types.emplace_back(StorageOperation::kWrite);
  }
  if (enable_reads) {
    operation_types.emplace_back(StorageOperation::kRead);
  }
  if (operation_types.empty()) {
    operation_types.emplace_back(StorageOperation::kWrite);
    operation_types.emplace_back(StorageOperation::kRead);
  }
  if (enable_vpc) {
    // TODO(tobodner): We probably want to pass it to the LambdaBenchmarkConfiguration.
    std::cout << "VPC enabled." << std::endl;
  }

  for (const auto after_repetition_delay_min : after_repetition_delays_min) {
    std::vector<std::function<void()>> after_repetition_callbacks;
    after_repetition_callbacks.reserve(repetition_count);

    for (size_t i = 0; i < repetition_count - 1; ++i) {
      after_repetition_callbacks.emplace_back([after_repetition_delay_min]() {
        std::this_thread::sleep_for(std::chrono::minutes(after_repetition_delay_min));
      });
    }

    after_repetition_callbacks.emplace_back([]() {});

    for (const auto function_instance_size_mb : function_instance_sizes_mb) {
      for (const auto concurrent_instance_count : concurrent_instance_counts) {
        for (const auto operation_type : operation_types) {
          for (const auto storage_type : storage_types) {
            const std::string efs_arn = storage_type == StorageSystem::kEfs ? kEfsArn.data() : "";
            const auto config = std::make_shared<LambdaBenchmarkConfig>(
                kFunctionName, "", Warmup::kNo, DistinctFunctionPerRepetition::kYes, EventQueue::kNo, false,
                function_instance_size_mb, efs_arn, concurrent_instance_count, repetition_count,
                after_repetition_callbacks);
            const LambdaStorageBenchmarkParameters parameters{
                {function_instance_size_mb, concurrent_instance_count, repetition_count},
                {object_size_kb, object_count, operation_type, storage_type, enable_s3_eoz}};

            configs_.emplace_back(parameters, config);
          }
        }
      }
    }
  }
}

const Aws::String& LambdaStorageBenchmark::Name() const { return kName; }

void LambdaStorageBenchmark::Setup() {}

void LambdaStorageBenchmark::Teardown() {}

std::vector<std::shared_ptr<Aws::IOStream>> LambdaStorageBenchmark::GeneratePayloads(
    const LambdaStorageBenchmarkParameters& parameters, const std::string& sqs_queue_url) {
  std::vector<std::shared_ptr<Aws::IOStream>> payloads;
  payloads.reserve(parameters.base_parameters.concurrent_instance_count);

  for (size_t i = 0; i < parameters.base_parameters.concurrent_instance_count; ++i) {
    const auto payload_value =
        Aws::Utils::Json::JsonValue()
            .WithInteger("concurrent_instance_count", parameters.base_parameters.concurrent_instance_count)
            .WithInteger("object_size_bytes", KBToByte(parameters.storage_parameters.object_size_kb))
            .WithInteger("object_count", parameters.storage_parameters.object_count)
            .WithString("operation_type",
                        std::string(magic_enum::enum_name(parameters.storage_parameters.operation_type)))
            .WithString("storage_type", std::string(magic_enum::enum_name(parameters.storage_parameters.system_type)))
            .WithString("container_name", parameters.storage_parameters.is_s3_express ? kS3ExpressBucket : kContainer)
            .WithString("object_prefix",
                        parameters.storage_parameters.is_s3_shared ? kSharedObjectPrefix : kObjectPrefix)
            .WithString("sqs_queue_url", sqs_queue_url);

    payloads.emplace_back(std::make_shared<Aws::StringStream>(payload_value.View().WriteCompact()));
  }

  return payloads;
}

LambdaBenchmarkOutput& LambdaStorageBenchmark::AddParameters(LambdaBenchmarkOutput& output,
                                                             const LambdaStorageBenchmarkParameters& parameters) {
  return dynamic_cast<LambdaBenchmarkOutput&>(
      output.WithInt64Argument("instance_size_mb", parameters.base_parameters.function_instance_size_mb)
          .WithInt64Argument("concurrent_instance_count", parameters.base_parameters.concurrent_instance_count)
          .WithInt64Argument("repetition_count", parameters.base_parameters.repetition_count)
          .WithInt64Argument("object_size_kb", parameters.storage_parameters.object_size_kb)
          .WithInt64Argument("object_count", parameters.storage_parameters.object_count)
          .WithStringArgument("operation_type",
                              std::string(magic_enum::enum_name(parameters.storage_parameters.operation_type)))
          .WithStringArgument("storage_type",
                              std::string(magic_enum::enum_name(parameters.storage_parameters.system_type)))
          .WithStringArgument("container_name",
                              parameters.storage_parameters.is_s3_express ? kS3ExpressBucket : kContainer)
          .WithStringArgument("object_prefix",
                              parameters.storage_parameters.is_s3_shared ? kSharedObjectPrefix : kObjectPrefix));
}

Aws::Utils::Json::JsonValue LambdaStorageBenchmark::GenerateResultOutput(
    const std::shared_ptr<LambdaBenchmarkResult>& result, const LambdaStorageBenchmarkParameters& parameters) const {
  std::vector<double> request_latencies_ms;
  const auto& repetition_results = result->GetRepetitionResults();
  request_latencies_ms.reserve(repetition_results.size() * repetition_results.front().GetInvocationResults().size() *
                               parameters.storage_parameters.object_count);

  std::vector<double> invocation_throughputs_mbps;
  invocation_throughputs_mbps.reserve(repetition_results.size() *
                                      repetition_results.front().GetInvocationResults().size());

  size_t succeeded_invocation_count = 0;

  for (const auto& repetition_result : repetition_results) {
    for (const auto& invocation_result : repetition_result.GetInvocationResults()) {
      if (invocation_result.IsSuccess()) {
        const double invocation_result_measurement_duration_ms =
            invocation_result.GetResponseBody().GetDouble("measurement_duration_ms");
        const double invocation_throughput_mbps =
            static_cast<double>(KBToMB(parameters.storage_parameters.object_size_kb) *
                                parameters.storage_parameters.object_count) /
            (invocation_result_measurement_duration_ms / 1000);
        invocation_throughputs_mbps.emplace_back(invocation_throughput_mbps);

        const auto invocation_result_request_latencies_ms =
            invocation_result.GetResponseBody().GetArray("request_latencies_ms");
        for (size_t i = 0; i < invocation_result_request_latencies_ms.GetLength(); ++i) {
          request_latencies_ms.emplace_back(invocation_result_request_latencies_ms[i].AsDouble());
        }
        succeeded_invocation_count++;
      }
    }
    if (invocation_throughputs_mbps.empty()) {
      invocation_throughputs_mbps.emplace_back(-1.0);
    }
    if (request_latencies_ms.empty()) {
      request_latencies_ms.emplace_back(-1.0);
    }
  }

  const BenchmarkResultAggregate invocation_throughput_aggregates(invocation_throughputs_mbps, 2);
  const BenchmarkResultAggregate request_latency_aggregates(request_latencies_ms, 2);

  LambdaBenchmarkOutput output(Name(), parameters.base_parameters, result);

  // Benchmark parameters.
  return AddParameters(output, parameters)
      // Benchmark metrics.
      .WithInt64Metric("succeeded_invocation_count", static_cast<long long>(succeeded_invocation_count))
      .WithDoubleMetric("request_latency_minimum_ms", request_latency_aggregates.GetMinimum())
      .WithDoubleMetric("request_latency_maximum_ms", request_latency_aggregates.GetMaximum())
      .WithDoubleMetric("request_latency_average_ms", request_latency_aggregates.GetAverage())
      .WithDoubleMetric("request_latency_median_ms", request_latency_aggregates.GetMedian())
      .WithDoubleMetric("request_latency_percentile_5_ms", request_latency_aggregates.GetPercentile(5))
      .WithDoubleMetric("request_latency_percentile_25_ms", request_latency_aggregates.GetPercentile(25))
      .WithDoubleMetric("request_latency_percentile_75_ms", request_latency_aggregates.GetPercentile(75))
      .WithDoubleMetric("request_latency_percentile_90_ms", request_latency_aggregates.GetPercentile(90))
      .WithDoubleMetric("request_latency_percentile_95_ms", request_latency_aggregates.GetPercentile(95))
      .WithDoubleMetric("request_latency_percentile_99_ms", request_latency_aggregates.GetPercentile(99))
      .WithDoubleMetric("request_latency_percentile_99.9_ms", request_latency_aggregates.GetPercentile(99.9))
      .WithDoubleMetric("request_latency_percentile_99.99_ms", request_latency_aggregates.GetPercentile(99.99))
      .WithDoubleMetric("request_latency_std_dev_ms", request_latency_aggregates.GetStandardDeviation())
      .WithDoubleMetric("invocation_throughput_minimum_mbps", invocation_throughput_aggregates.GetMinimum())
      .WithDoubleMetric("invocation_throughput_maximum_mbps", invocation_throughput_aggregates.GetMaximum())
      .WithDoubleMetric("invocation_throughput_average_mbps", invocation_throughput_aggregates.GetAverage())
      .WithDoubleMetric("invocation_throughput_median_mbps", invocation_throughput_aggregates.GetMedian())
      .WithDoubleMetric("invocation_throughput_percentile_0.01_mbps",
                        invocation_throughput_aggregates.GetPercentile(0.01))
      .WithDoubleMetric("invocation_throughput_percentile_0.1_mbps",
                        invocation_throughput_aggregates.GetPercentile(0.1))
      .WithDoubleMetric("invocation_throughput_percentile_1_mbps", invocation_throughput_aggregates.GetPercentile(1))
      .WithDoubleMetric("invocation_throughput_percentile_10_mbps", invocation_throughput_aggregates.GetPercentile(10))
      .WithDoubleMetric("invocation_throughput_std_dev_mbps", invocation_throughput_aggregates.GetStandardDeviation())
      // Repetition metrics.
      .WithDoubleRepetitionMetric([&](const LambdaBenchmarkRepetitionResult& repetition_result) {
        std::vector<double> invocation_measurement_starts_ms;
        invocation_measurement_starts_ms.reserve(repetition_result.GetInvocationResults().size());
        std::vector<double> invocation_measurement_ends_ms;
        invocation_measurement_ends_ms.reserve(repetition_result.GetInvocationResults().size());
        for (auto const& invocation_result : repetition_result.GetInvocationResults()) {
          if (invocation_result.IsSuccess()) {
            const double invocation_measurement_start_ms =
                invocation_result.GetResponseBody().GetDouble("measurement_start_ms");
            invocation_measurement_starts_ms.emplace_back(invocation_measurement_start_ms);
            const double invocation_measurement_end_ms =
                invocation_result.GetResponseBody().GetDouble("measurement_end_ms");
            invocation_measurement_ends_ms.emplace_back(invocation_measurement_end_ms);
          }
        }

        const double repetition_measurement_start_ms =
            *std::min_element(invocation_measurement_starts_ms.begin(), invocation_measurement_starts_ms.end());
        const double repetition_measurement_end_ms =
            *std::max_element(invocation_measurement_ends_ms.begin(), invocation_measurement_ends_ms.end());
        const double repetition_measurement_duration_ms =
            repetition_measurement_end_ms - repetition_measurement_start_ms;

        const double repetition_throughput_mbps =
            static_cast<double>(KBToMB(parameters.storage_parameters.object_size_kb) *
                                parameters.storage_parameters.object_count *
                                parameters.base_parameters.concurrent_instance_count) /
            (repetition_measurement_duration_ms / 1000);
        return std::make_tuple("repetition_throughput_mbps", repetition_throughput_mbps);
      })
      .WithDoubleRepetitionMetric([&](const LambdaBenchmarkRepetitionResult& repetition_result) {
        std::vector<double> invocation_measurement_starts_ms;
        invocation_measurement_starts_ms.reserve(repetition_result.GetInvocationResults().size());
        std::vector<double> invocation_measurement_ends_ms;
        invocation_measurement_ends_ms.reserve(repetition_result.GetInvocationResults().size());
        for (auto const& invocation_result : repetition_result.GetInvocationResults()) {
          if (invocation_result.IsSuccess()) {
            const double invocation_measurement_start_ms =
                invocation_result.GetResponseBody().GetDouble("measurement_start_ms");
            invocation_measurement_starts_ms.emplace_back(invocation_measurement_start_ms);
            const double invocation_measurement_end_ms =
                invocation_result.GetResponseBody().GetDouble("measurement_end_ms");
            invocation_measurement_ends_ms.emplace_back(invocation_measurement_end_ms);
          }
        }

        const double repetition_measurement_start_ms =
            *std::min_element(invocation_measurement_starts_ms.begin(), invocation_measurement_starts_ms.end());
        const double repetition_measurement_end_ms =
            *std::max_element(invocation_measurement_ends_ms.begin(), invocation_measurement_ends_ms.end());
        const double repetition_measurement_duration_ms =
            repetition_measurement_end_ms - repetition_measurement_start_ms;

        const double repetition_iops = static_cast<double>(parameters.storage_parameters.object_count *
                                                           parameters.base_parameters.concurrent_instance_count) /
                                       (repetition_measurement_duration_ms / 1000);
        return std::make_tuple("repetition_iops", repetition_iops);
      })
      .WithObjectRepetitionMetric([&](const LambdaBenchmarkRepetitionResult& repetition_result) {
        std::vector<double> repetition_invocation_throughputs_mbps;
        repetition_invocation_throughputs_mbps.reserve(repetition_result.GetInvocationResults().size());

        for (auto const& invocation_result : repetition_result.GetInvocationResults()) {
          if (invocation_result.IsSuccess()) {
            const double invocation_result_measurement_duration_ms =
                invocation_result.GetResponseBody().GetDouble("measurement_duration_ms");
            const double invocation_throughput_mbps =
                static_cast<double>(KBToMB(parameters.storage_parameters.object_size_kb) *
                                    parameters.storage_parameters.object_count) /
                (invocation_result_measurement_duration_ms / 1000);
            repetition_invocation_throughputs_mbps.emplace_back(invocation_throughput_mbps);
          }
        }
        if (repetition_invocation_throughputs_mbps.empty()) {
          repetition_invocation_throughputs_mbps.emplace_back(-1.0);
        }

        const BenchmarkResultAggregate repetition_invocation_throughput_aggregates(
            repetition_invocation_throughputs_mbps);

        return std::make_tuple("repetition_metrics_invocation_throughput",
                               Aws::Utils::Json::JsonValue()
                                   .WithDouble("repetition_invocation_throughput_minimum_mbps",
                                               repetition_invocation_throughput_aggregates.GetMinimum())
                                   .WithDouble("repetition_invocation_throughput_maximum_mbps",
                                               repetition_invocation_throughput_aggregates.GetMaximum())
                                   .WithDouble("repetition_invocation_throughput_average_mbps",
                                               repetition_invocation_throughput_aggregates.GetAverage())
                                   .WithDouble("repetition_invocation_throughput_median_mbps",
                                               repetition_invocation_throughput_aggregates.GetMedian())
                                   .WithDouble("repetition_invocation_throughput_percentile_0.01_mbps",
                                               repetition_invocation_throughput_aggregates.GetPercentile(0.01))
                                   .WithDouble("repetition_invocation_throughput_percentile_0.1_mbps",
                                               repetition_invocation_throughput_aggregates.GetPercentile(0.1))
                                   .WithDouble("repetition_invocation_throughput_percentile_1_mbps",
                                               repetition_invocation_throughput_aggregates.GetPercentile(1))
                                   .WithDouble("repetition_invocation_throughput_percentile_10_mbps",
                                               repetition_invocation_throughput_aggregates.GetPercentile(10))
                                   .WithDouble("repetition_invocation_throughput_std_dev_mbps",
                                               repetition_invocation_throughput_aggregates.GetStandardDeviation()));
      })
      .WithObjectRepetitionMetric([&](const LambdaBenchmarkRepetitionResult& repetition_result) {
        std::vector<double> repetition_request_latencies_ms;
        repetition_request_latencies_ms.reserve(repetition_result.GetInvocationResults().size() *
                                                parameters.storage_parameters.object_count);

        for (const auto& invocation_result : repetition_result.GetInvocationResults()) {
          if (invocation_result.IsSuccess()) {
            const auto invocation_result_request_latencies_ms =
                invocation_result.GetResponseBody().GetArray("request_latencies_ms");
            for (size_t i = 0; i < invocation_result_request_latencies_ms.GetLength(); ++i) {
              repetition_request_latencies_ms.emplace_back(invocation_result_request_latencies_ms[i].AsDouble());
            }
          }
        }
        if (repetition_request_latencies_ms.empty()) {
          repetition_request_latencies_ms.emplace_back(-1.0);
        }

        const BenchmarkResultAggregate repetition_request_latency_aggregates(repetition_request_latencies_ms);

        return std::make_tuple(
            "repetition_metrics_request_latency",
            Aws::Utils::Json::JsonValue()
                .WithDouble("repetition_request_latency_minimum_ms", repetition_request_latency_aggregates.GetMinimum())
                .WithDouble("repetition_request_latency_maximum_ms", repetition_request_latency_aggregates.GetMaximum())
                .WithDouble("repetition_request_latency_average_ms", repetition_request_latency_aggregates.GetAverage())
                .WithDouble("repetition_request_latency_median_ms", repetition_request_latency_aggregates.GetMedian())
                .WithDouble("repetition_request_latency_percentile_5_ms",
                            repetition_request_latency_aggregates.GetPercentile(5))
                .WithDouble("repetition_request_latency_percentile_25_ms",
                            repetition_request_latency_aggregates.GetPercentile(25))
                .WithDouble("repetition_request_latency_percentile_75_ms",
                            repetition_request_latency_aggregates.GetPercentile(75))
                .WithDouble("repetition_request_latency_percentile_90_ms",
                            repetition_request_latency_aggregates.GetPercentile(90))
                .WithDouble("repetition_request_latency_percentile_95_ms",
                            repetition_request_latency_aggregates.GetPercentile(95))
                .WithDouble("repetition_request_latency_percentile_99_ms",
                            repetition_request_latency_aggregates.GetPercentile(99))
                .WithDouble("repetition_request_latency_percentile_99.9_ms",
                            repetition_request_latency_aggregates.GetPercentile(99.9))
                .WithDouble("repetition_request_latency_percentile_99.99_ms",
                            repetition_request_latency_aggregates.GetPercentile(99.99))
                .WithDouble("repetition_request_latency_std_dev_ms",
                            repetition_request_latency_aggregates.GetStandardDeviation()));
      })
      .WithObjectRepetitionMetric([&](const LambdaBenchmarkRepetitionResult& repetition_result) {
        size_t finished_request_count = 0;
        size_t succeeded_request_count = 0;
        size_t retried_request_count = 0;
        size_t failed_request_count = 0;

        for (auto const& invocation_result : repetition_result.GetInvocationResults()) {
          if (invocation_result.IsSuccess()) {
            if (parameters.storage_parameters.system_type == StorageSystem::kEfs) {
              // TODO(tobodner): Implement lower-level filesystem checks.
              succeeded_request_count =
                  parameters.base_parameters.concurrent_instance_count * parameters.storage_parameters.object_count;
              finished_request_count = succeeded_request_count;
            } else {
              std::string request_name;
              if (parameters.storage_parameters.system_type == StorageSystem::kS3) {
                if (parameters.storage_parameters.operation_type == StorageOperation::kRead) {
                  request_name = "S3:GetObject";
                } else if (parameters.storage_parameters.operation_type == StorageOperation::kWrite) {
                  request_name = "S3:PutObject";
                } else {
                  Fail("Unknown operation type.");
                }
              } else if (parameters.storage_parameters.system_type == StorageSystem::kDynamoDb) {
                if (parameters.storage_parameters.operation_type == StorageOperation::kRead) {
                  request_name = "DynamoDB:GetItem";
                } else if (parameters.storage_parameters.operation_type == StorageOperation::kWrite) {
                  request_name = "DynamoDB:PutItem";
                } else {
                  Fail("Unknown operation type.");
                }
              }
              const auto& metering = invocation_result.GetResponseBody().GetObject("metering");
              if (metering.KeyExists(request_name)) {
                const auto& request_counts = metering.GetObject(request_name);
                finished_request_count += request_counts.GetInt64("finished");
                succeeded_request_count += request_counts.GetInt64("succeeded");
                retried_request_count += request_counts.GetInt64("retried");
                failed_request_count += request_counts.GetInt64("failed");
              }
            }
          }
        }

        return std::make_tuple("repetition_metrics_request_tracking",
                               Aws::Utils::Json::JsonValue()
                                   .WithInteger("finished_request_count", finished_request_count)
                                   .WithInteger("succeeded_request_count", succeeded_request_count)
                                   .WithInteger("retried_request_count", retried_request_count)
                                   .WithInteger("failed_request_count", failed_request_count));
      })
      // Invocation metrics.
      .WithObjectInvocationMetric([](const LambdaInvocationResult& invocation_result) {
        const auto invocation_result_request_latencies_ms =
            invocation_result.GetResponseBody().GetArray("request_latencies_ms");

        Aws::Utils::Array<Aws::Utils::Json::JsonValue> invocation_request_latencies_ms(
            invocation_result_request_latencies_ms.GetLength());

        for (size_t i = 0; i < invocation_result_request_latencies_ms.GetLength(); ++i) {
          invocation_request_latencies_ms[i] =
              Aws::Utils::Json::JsonValue().AsDouble(invocation_result_request_latencies_ms[i].AsDouble());
        }

        return std::make_tuple("invocation_request_latencies_ms",
                               Aws::Utils::Json::JsonValue().AsArray(invocation_request_latencies_ms));
      })
      .WithDoubleInvocationMetric([](const LambdaInvocationResult& invocation_result) {
        return std::make_tuple("measurement_start_ms",
                               invocation_result.GetResponseBody().GetDouble("measurement_start_ms"));
      })
      .WithDoubleInvocationMetric([](const LambdaInvocationResult& invocation_result) {
        return std::make_tuple("measurement_end_ms",
                               invocation_result.GetResponseBody().GetDouble("measurement_end_ms"));
      })
      .WithDoubleInvocationMetric([](const LambdaInvocationResult& invocation_result) {
        return std::make_tuple("measurement_duration_ms",
                               invocation_result.GetResponseBody().GetDouble("measurement_duration_ms"));
      })
      .WithDoubleInvocationMetric([&](const LambdaInvocationResult& invocation_result) {
        const double invocation_result_measurement_duration_ms =
            invocation_result.GetResponseBody().GetDouble("measurement_duration_ms");
        const double invocation_throughput_mbps =
            static_cast<double>(KBToMB(parameters.storage_parameters.object_size_kb) *
                                parameters.storage_parameters.object_count) /
            (invocation_result_measurement_duration_ms / 1000);
        return std::make_tuple("invocation_throughput_mbps", invocation_throughput_mbps);
      })
      .WithStringInvocationMetric([](const LambdaInvocationResult& invocation_result) {
        return std::make_tuple("invocation_start_timestamp",
                               invocation_result.GetResponseBody().GetString("invocation_start_timestamp"));
      })
      .WithStringInvocationMetric([](const LambdaInvocationResult& invocation_result) {
        return std::make_tuple("measurement_start_timestamp",
                               invocation_result.GetResponseBody().GetString("measurement_start_timestamp"));
      })
      .WithStringInvocationMetric([](const LambdaInvocationResult& invocation_result) {
        return std::make_tuple("measurement_end_timestamp",
                               invocation_result.GetResponseBody().GetString("measurement_end_timestamp"));
      })
      .Build();
}

std::string LambdaStorageBenchmark::SetupSqsQueue() const {
  const std::string queue_name = kName + "-" + RandomString(10);
  Aws::SQS::Model::CreateQueueRequest request;
  request.WithQueueName(queue_name);
  const auto outcome = sqs_client_->CreateQueue(request);
  Assert(outcome.IsSuccess(), outcome.GetError().GetMessage());
  return outcome.GetResult().GetQueueUrl();
}

Aws::Utils::Array<Aws::Utils::Json::JsonValue> LambdaStorageBenchmark::OnRun(
    const std::shared_ptr<LambdaBenchmarkRunner>& runner) {
  Setup();

  std::vector<std::shared_ptr<LambdaBenchmarkResult>> results;
  results.reserve(configs_.size());

  for (const auto& config : configs_) {
    const std::string sqs_queue_url = SetupSqsQueue();
    config.second->SetPayloads(GeneratePayloads(config.first, sqs_queue_url));
    results.push_back(runner->RunLambdaConfig(config.second));
    results.back()->ValidateInvocationResults(kExpectedInvocationResponseTemplate);
    sqs_client_->DeleteQueue(Aws::SQS::Model::DeleteQueueRequest().WithQueueUrl(sqs_queue_url));
  }

  Teardown();

  Aws::Utils::Array<Aws::Utils::Json::JsonValue> results_array(results.size());

  for (size_t i = 0; i < results.size(); ++i) {
    results_array[i] = GenerateResultOutput(results[i], configs_[i].first);
  }

  return results_array;
}

}  // namespace skyrise
