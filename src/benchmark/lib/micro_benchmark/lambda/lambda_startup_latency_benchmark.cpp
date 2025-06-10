#include "lambda_startup_latency_benchmark.hpp"

#include <algorithm>
#include <fstream>
#include <iostream>
#include <memory>
#include <numeric>
#include <regex>
#include <thread>

#include <aws/core/Aws.h>
#include <aws/core/Region.h>
#include <aws/core/utils/base64/Base64.h>
#include <aws/core/utils/logging/LogMacros.h>

#include "benchmark_result_aggregate.hpp"
#include "constants.hpp"
#include "function/function_utils.hpp"
#include "lambda_benchmark_output.hpp"
#include "storage/backend/s3_utils.hpp"
#include "utils/assert.hpp"
#include "utils/string.hpp"

namespace skyrise {

namespace {

const Aws::String kName = "lambdaStartupLatencyBenchmark";
const Aws::String kBucketName = kName + "-" + RandomString(8, kCharacterSetLower + kCharacterSetDecimal);
const std::vector<Aws::String> kFunctionPackagesAsS3Object{
    "S3_skyriseSizedFunction10MB", "S3_skyriseSizedFunction20MB", "S3_skyriseSizedFunction30MB",
    "S3_skyriseSizedFunction40MB", "S3_skyriseSizedFunction50MB", "S3_skyriseSizedFunction100MB"};
const std::vector<Aws::String> kFunctionPackagesAsZipFile{"skyriseMinimalFunction",   "skyriseSizedFunction10MB",
                                                          "skyriseSizedFunction20MB", "skyriseSizedFunction30MB",
                                                          "skyriseSizedFunction40MB", "skyriseSizedFunction50MB"};
constexpr double kTraceProvisioningFactor = 1.4;
constexpr size_t kTraceRetrievalDelayMs = 10;

}  // namespace

LambdaStartupLatencyBenchmark::LambdaStartupLatencyBenchmark(std::shared_ptr<const CostCalculator> cost_calculator,
                                                             std::shared_ptr<const Aws::S3::S3Client> s3_client,
                                                             std::shared_ptr<const Aws::XRay::XRayClient> xray_client,
                                                             const std::vector<size_t>& function_instance_sizes_mb,
                                                             const std::vector<size_t>& concurrent_instance_counts,
                                                             const size_t repetition_count,
                                                             const std::vector<DeploymentType>& deployment_types)
    : LambdaBenchmark(std::move(cost_calculator)),
      s3_client_(std::move(s3_client)),
      xray_client_(std::move(xray_client)),
      function_instance_sizes_mb_(function_instance_sizes_mb),
      concurrent_instance_counts_(concurrent_instance_counts),
      repetition_count_(repetition_count),
      deployment_types_(deployment_types) {}

Aws::Utils::Array<Aws::Utils::Json::JsonValue> LambdaStartupLatencyBenchmark::OnRun(
    const std::shared_ptr<LambdaBenchmarkRunner>& runner) {
  Setup();

  std::vector<std::shared_ptr<LambdaBenchmarkResult>> results;
  results.reserve(configs_.size());

  for (const auto& config : configs_) {
    results.emplace_back(runner->RunLambdaConfig(config.second));
  }

  std::vector<std::shared_ptr<std::unordered_map<Aws::String, LambdaSegmentDurations>>> result_segments;
  result_segments.reserve(configs_.size());

  for (size_t i = 0; i < results.size(); ++i) {
    const auto& result = results[i];
    const auto& parameters = configs_[i].first;
    LambdaSegmentAnalyzer function_segment_analyzer(xray_client_);

    const auto config_result_segments_futures =
        std::make_shared<std::vector<std::pair<Aws::String, std::future<LambdaSegmentDurations>>>>();
    config_result_segments_futures->reserve(
        parameters.base_parameters.repetition_count *
        (parameters.base_parameters.concurrent_instance_count * kTraceProvisioningFactor));

    for (const auto& repetition_result : result->GetRepetitionResults()) {
      for (const auto& invocation_result : repetition_result.GetInvocationResults()) {
        if (!invocation_result.IsSuccess()) {
          continue;
        }

        config_result_segments_futures->emplace_back(
            invocation_result.GetInstanceId(), std::async([&]() {
              std::map<Aws::String, Aws::Utils::Json::JsonValue> segments;

              try {
                Assert(invocation_result.HasResultLog(), "InvocationResult must contain LogResult.");
                Assert(invocation_result.GetResultLog()->HasXrayTraceId(), "LogResult must contain Xray TraceId.");

                const auto trace_id = invocation_result.GetResultLog()->GetXrayTraceId();
                const auto trace = function_segment_analyzer.GetTraces({trace_id})[trace_id];
                segments = LambdaSegmentAnalyzer::GetSegments(trace);
              } catch (const std::exception& e) {
                AWS_LOGSTREAM_ERROR(kBenchmarkTag.c_str(), e.what());
              }

              return LambdaSegmentAnalyzer::CalculateLambdaSegmentDurations(segments, invocation_result.GetStartPoint(),
                                                                            invocation_result.GetEndPoint());
            }));

        // Reduce throttled exceptions during trace retrieval
        std::this_thread::sleep_for(std::chrono::milliseconds(kTraceRetrievalDelayMs));
      }
    }

    AWS_LOGSTREAM_INFO(kBenchmarkTag.c_str(), "Get traces...");

    const auto config_result_segments = std::make_shared<std::unordered_map<Aws::String, LambdaSegmentDurations>>();
    config_result_segments->reserve(parameters.base_parameters.repetition_count *
                                    (parameters.base_parameters.concurrent_instance_count * kTraceProvisioningFactor));

    for (auto& future_segment_result : *config_result_segments_futures) {
      auto latency_segments = future_segment_result.second.get();

      // Use functions with initialization for coldstart testing and without initialization for warmstart testing
      if ((parameters.is_warm != Warmup::kNo ? latency_segments["initialization_latency_ms"].count() == 0 &&
                                                   latency_segments["function_total_latency_ms"].count() > 0
                                             : latency_segments["initialization_latency_ms"].count() > 0) &&
          config_result_segments->size() <
              parameters.base_parameters.repetition_count * parameters.base_parameters.concurrent_instance_count) {
        (*config_result_segments)[future_segment_result.first] = latency_segments;
      }
    }

    result_segments.emplace_back(config_result_segments);

    result->SetTraceCost(cost_calculator_->CalculateCostXray(
        parameters.base_parameters.repetition_count * parameters.base_parameters.concurrent_instance_count *
            kTraceProvisioningFactor,
        function_segment_analyzer.GetNumScannedTraces(), function_segment_analyzer.GetNumAccessedTraces()));
  }

  Aws::Utils::Array<Aws::Utils::Json::JsonValue> outputs(results.size());

  for (size_t i = 0; i < results.size(); ++i) {
    const auto& parameters = configs_[i].first;
    const auto& result = results[i];

    if (result_segments[i]->size() <
        parameters.base_parameters.repetition_count * parameters.base_parameters.concurrent_instance_count) {
      Fail(parameters.package_name + ": Only " + std::to_string(result_segments[i]->size()) + " traces of " +
           std::to_string(parameters.base_parameters.repetition_count *
                          parameters.base_parameters.concurrent_instance_count) +
           " function invocations could be retrieved.");
    } else {
      AWS_LOGSTREAM_INFO(kBenchmarkTag.c_str(),
                         parameters.package_name + ": " + std::to_string(result_segments[i]->size()) + " traces of " +
                             std::to_string(parameters.base_parameters.repetition_count *
                                            parameters.base_parameters.concurrent_instance_count) +
                             " function invocations will be evaluated.");
    }

    outputs[i] = GenerateResultOutput(result, result_segments[i], parameters);
  }

  Teardown();

  return outputs;
}

void LambdaStartupLatencyBenchmark::Setup() {
  std::vector<Aws::String> package_names;

  if (std::ranges::find(deployment_types_, DeploymentType::kS3Object) != deployment_types_.end()) {
    std::vector<Aws::String> s3_package_names;
    const std::regex s3_package_name_regex("S3_([^-]*)");

    for (const auto& package_name : kFunctionPackagesAsS3Object) {
      package_names.emplace_back(package_name);

      std::smatch matches;
      if (std::regex_search(package_name, matches, s3_package_name_regex)) {
        s3_package_names.emplace_back(matches[1]);
      }
    }

    std::vector<std::tuple<Aws::String, std::shared_ptr<Aws::IOStream>, size_t>> s3_objects;
    s3_objects.reserve(s3_package_names.size());

    for (const auto& s3_package_name : s3_package_names) {
      const std::shared_ptr<Aws::IOStream> package_file = Aws::MakeShared<Aws::FStream>(
          s3_package_name.c_str(), GetProjectDirectoryPath() + "pkg/" + s3_package_name + ".zip",
          std::ios_base::in | std::ios_base::binary);

      auto file_size = package_file->tellg();
      package_file->seekg(0, std::ios::end);
      file_size = package_file->tellg() - file_size;
      package_file->seekg(0, std::ios::beg);

      s3_objects.emplace_back(s3_package_name, package_file, file_size);
    }

    CreateS3BucketIfNotExists(s3_client_, kBucketName);
    EmptyS3Bucket(s3_client_, kBucketName);
    UploadObjectsToS3Parallel(s3_client_, s3_objects, kBucketName);
  }
  if (std::ranges::find(deployment_types_, DeploymentType::kLocalZipFile) != deployment_types_.end()) {
    package_names.insert(package_names.end(), kFunctionPackagesAsZipFile.begin(), kFunctionPackagesAsZipFile.end());
  }

  const std::array<Warmup, 2> warmup_options{Warmup::kNo, Warmup::kYesOnEveryRepetition};

  for (const auto& package_name : package_names) {
    for (const auto function_instance_size_mb : function_instance_sizes_mb_) {
      for (const auto concurrent_instance_count : concurrent_instance_counts_) {
        for (const auto is_warm : warmup_options) {
          const auto config = std::make_shared<LambdaBenchmarkConfig>(LambdaBenchmarkConfig{
              package_name,
              kBucketName,
              is_warm,
              is_warm != Warmup::kNo ? DistinctFunctionPerRepetition::kNo : DistinctFunctionPerRepetition::kYes,
              EventQueue::kNo,
              true,
              function_instance_size_mb,
              "",
              static_cast<size_t>(concurrent_instance_count * kTraceProvisioningFactor),
              repetition_count_,
              {}});

          const auto payload = std::make_shared<Aws::StringStream>(
              Aws::Utils::Json::JsonValue().WithBool("sleep", true).View().WriteCompact());
          config->SetOnePayloadForAllFunctions(payload);

          configs_.emplace_back(
              LambdaStartupBenchmarkParameters{
                  .base_parameters = {.function_instance_size_mb = function_instance_size_mb,
                                      .concurrent_instance_count = concurrent_instance_count,
                                      .repetition_count = repetition_count_,
                                      .after_repetition_delay_min = 0},
                  .package_name = package_name,
                  .payload_size_bytes = 0,
                  .is_event = EventQueue::kNo,
                  .is_warm = is_warm},
              config);
        }
      }
    }
  }
}

void LambdaStartupLatencyBenchmark::Teardown() {
  if (std::ranges::find(deployment_types_, DeploymentType::kS3Object) != deployment_types_.end()) {
    EmptyAndDeleteS3Bucket(s3_client_, kBucketName);
  }

  configs_.clear();
  configs_.shrink_to_fit();
}

LambdaBenchmarkOutput& LambdaStartupLatencyBenchmark::AddParameters(
    LambdaBenchmarkOutput& output, const LambdaStartupBenchmarkParameters& parameters) {
  return dynamic_cast<LambdaBenchmarkOutput&>(
      output.WithInt64Argument("function_instance_size_mb", parameters.base_parameters.function_instance_size_mb)
          .WithInt64Argument("concurrent_instance_count", parameters.base_parameters.concurrent_instance_count)
          .WithInt64Argument("repetition_count", parameters.base_parameters.repetition_count)
          .WithStringArgument("function_package_name", parameters.package_name)
          .WithBoolArgument("warmup_flag", parameters.is_warm != Warmup::kNo));
}

Aws::Utils::Json::JsonValue LambdaStartupLatencyBenchmark::GenerateResultOutput(
    const std::shared_ptr<LambdaBenchmarkResult>& result,
    const std::shared_ptr<std::unordered_map<Aws::String, LambdaSegmentDurations>>& result_segments,
    const LambdaStartupBenchmarkParameters& parameters) const {
  LambdaBenchmarkOutput output(Name(), parameters.base_parameters, result);
  // Benchmark parameters.
  AddParameters(output, parameters);

  // Benchmark metrics.
  const auto& segments = LambdaSegmentAnalyzer::CreateLambdaSegmentDurations();

  for (const auto& segment : segments) {
    std::vector<double> latencies;
    latencies.reserve(result_segments->size());

    std::transform(result_segments->cbegin(), result_segments->cend(), std::back_inserter(latencies),
                   [&segment](const std::pair<Aws::String, LambdaSegmentDurations>& segments) {
                     return std::chrono::duration<double, std::milli>(
                                std::chrono::duration<double>(segments.second.at(segment.first)))
                         .count();
                   });

    if (latencies.empty()) {
      continue;
    }

    const BenchmarkResultAggregate latency_aggregate(latencies);

    output.WithDoubleMetric(segment.first + "_latency_minimum_ms", latency_aggregate.GetMinimum())
        .WithDoubleMetric(segment.first + "_latency_maximum_ms", latency_aggregate.GetMaximum())
        .WithDoubleMetric(segment.first + "_latency_average_ms", latency_aggregate.GetAverage())
        .WithDoubleMetric(segment.first + "_latency_median_ms", latency_aggregate.GetMedian())
        .WithDoubleMetric(segment.first + "_latency_percentile_25_ms", latency_aggregate.GetPercentile(25))
        .WithDoubleMetric(segment.first + "_latency_percentile_75_ms", latency_aggregate.GetPercentile(75))
        .WithDoubleMetric(segment.first + "_latency_percentile_90_ms", latency_aggregate.GetPercentile(90))
        .WithDoubleMetric(segment.first + "_percentile_99_ms", latency_aggregate.GetPercentile(99))
        .WithDoubleMetric(segment.first + "_latency_latency_percentile_99.9_ms", latency_aggregate.GetPercentile(99.9))
        .WithDoubleMetric(segment.first + "_latency_percentile_99.99_ms", latency_aggregate.GetPercentile(99.99))
        .WithDoubleMetric(segment.first + "_latency_std_dev_ms", latency_aggregate.GetStandardDeviation());
  }

  for (const auto& segment : segments) {
    output.WithDoubleInvocationMetric([&segment, &result_segments](const LambdaInvocationResult& invocation_result) {
      return std::make_tuple(segment.first, std::chrono::duration<double, std::milli>(
                                                (*result_segments)[invocation_result.GetInstanceId()][segment.first])
                                                .count());
    });
  }

  return output.Build();
}

const Aws::String& LambdaStartupLatencyBenchmark::Name() const { return kName; }

}  // namespace skyrise
