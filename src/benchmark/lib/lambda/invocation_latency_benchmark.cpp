#include "invocation_latency_benchmark.hpp"

#include <algorithm>
#include <fstream>
#include <iostream>
#include <memory>
#include <numeric>
#include <regex>

#include <aws/core/Aws.h>
#include <aws/core/Region.h>
#include <aws/core/utils/base64/Base64.h>
#include <aws/core/utils/logging/LogMacros.h>

#include "benchmark_result_aggregate.hpp"
#include "utils/assert.hpp"

namespace skyrise {

InvocationLatencyBenchmark::InvocationLatencyBenchmark(
    std::shared_ptr<const Aws::XRay::XRayClient> xray_client, std::shared_ptr<const BenchmarkHelper> helper,
    std::shared_ptr<const CostCalculator> cost_calculator, const std::vector<size_t>& function_instance_mb_sizes,
    const std::vector<size_t>& invocation_counts, const std::vector<bool>& warm_modes,
    const std::vector<size_t>& sleep_ms_durations, const size_t repetition_count)
    : LambdaBenchmark(cost_calculator),
      xray_client_(std::move(xray_client)),
      helper_(std::move(helper)),
      cost_calculator_(std::move(cost_calculator)),
      function_instance_mb_sizes_(function_instance_mb_sizes),
      invocation_counts_(invocation_counts),
      warm_modes_(warm_modes),
      sleep_ms_durations_(sleep_ms_durations),
      repetition_count_(repetition_count),
      benchmark_cost_(0),
      function_segments_analyzer_(std::make_shared<FunctionSegmentsAnalyzer>(xray_client_)) {}

Aws::Utils::Array<Aws::Utils::Json::JsonValue> InvocationLatencyBenchmark::OnRun(
    const std::shared_ptr<LambdaBenchmarkRunner>& benchmark_runner) {
  Setup();

  std::vector<std::shared_ptr<LambdaBenchmarkResult>> benchmark_results;
  benchmark_results.reserve(benchmark_configs_.size());

  for (const auto& benchmark_config : benchmark_configs_) {
    benchmark_results.emplace_back(benchmark_runner->RunLambdaConfig(benchmark_config.second));
  }

  std::vector<std::shared_ptr<std::unordered_map<Aws::String, LambdaSegmentDurations>>> result_segments;
  result_segments.reserve(benchmark_configs_.size());

  for (size_t i = 0; i < benchmark_results.size(); ++i) {
    const auto& benchmark_parameters = benchmark_configs_[i].first;
    const auto& benchmark_result = benchmark_results[i];

    const auto config_result_segments_futures =
        std::make_shared<std::vector<std::pair<Aws::String, std::future<LambdaSegmentDurations>>>>();
    config_result_segments_futures->reserve(benchmark_parameters.repetition_count *
                                            (benchmark_parameters.invocation_count * kOverprovisioningCoefficient));

    for (const auto& benchmark_repetition : benchmark_result->GetBenchmarkRepetitions()) {
      for (const auto& invoke_result : benchmark_repetition.GetInvokeResults()) {
        if (!invoke_result.IsSuccess()) {
          continue;
        }

        config_result_segments_futures->emplace_back(
            invoke_result.GetInvokeId(), std::async([&]() {
              std::map<Aws::String, Aws::Utils::Json::JsonValue> segments;

              try {
                Assert(invoke_result.HasLogResult(), "InvokeResult must contain LogResult.");
                Assert(invoke_result.GetLogResult()->HasXrayTraceId(), "LogResult must contain Xray TraceId.");

                const auto trace_id = invoke_result.GetLogResult()->GetXrayTraceId();
                const auto trace = function_segments_analyzer_->GetTraces({trace_id})[trace_id];
                segments = FunctionSegmentsAnalyzer::GetSegments(trace);
              } catch (const std::exception& e) {
                AWS_LOGSTREAM_ERROR(kTag.c_str(), e.what());
              }

              return FunctionSegmentsAnalyzer::CalculateLambdaSegmentDurations(segments, invoke_result.GetStartPoint(),
                                                                               invoke_result.GetEndPoint());
            }));

        // Reduce throttled exceptions during trace retrieval
        std::this_thread::sleep_for(std::chrono::milliseconds(kTraceRetrievalDelayMs));
      }
    }

    AWS_LOGSTREAM_INFO(kTag.c_str(), "Get traces...");

    const auto config_result_segments = std::make_shared<std::unordered_map<Aws::String, LambdaSegmentDurations>>();
    config_result_segments->reserve(benchmark_parameters.repetition_count *
                                    (benchmark_parameters.invocation_count * kOverprovisioningCoefficient));

    for (auto& future_segment_result : *config_result_segments_futures) {
      auto latency_segments = future_segment_result.second.get();
      // Use functions with initialization for coldstart testing and without initialization for warmstart testing
      if ((benchmark_parameters.warm_mode
               ? latency_segments["initialization"].count() == 0 && latency_segments["function_total"].count() > 0
               : latency_segments["initialization"].count() > 0) &&
          config_result_segments->size() <
              benchmark_parameters.repetition_count * benchmark_parameters.invocation_count) {
        (*config_result_segments)[future_segment_result.first] = latency_segments;
      }
    }

    result_segments.emplace_back(config_result_segments);
  }

  Aws::Utils::Array<Aws::Utils::Json::JsonValue> benchmark_outputs(benchmark_results.size());

  for (size_t i = 0; i < benchmark_results.size(); ++i) {
    const auto& benchmark_parameters = benchmark_configs_[i].first;
    const auto& benchmark_result = benchmark_results[i];

    if (result_segments[i]->size() < benchmark_parameters.repetition_count * benchmark_parameters.invocation_count) {
      Fail(benchmark_parameters.function_package_name + ": Only " + std::to_string(result_segments[i]->size()) +
           " traces of " +
           std::to_string(benchmark_parameters.repetition_count * benchmark_parameters.invocation_count) +
           " function invocations could be retrieved.");
    } else {
      AWS_LOGSTREAM_INFO(kTag.c_str(), benchmark_parameters.function_package_name + ": " +
                                           std::to_string(result_segments[i]->size()) + " traces of " +
                                           std::to_string(benchmark_parameters.repetition_count *
                                                          benchmark_parameters.invocation_count) +
                                           " function invocations will be evaluated.");
    }

    benchmark_outputs[i] = GenerateResultOutput(benchmark_result, benchmark_parameters, result_segments[i]);
  }

  benchmark_cost_ += CalculateBenchmarkCost(benchmark_results);
  helper_->EmptyS3Bucket(kBenchmarkName);

  AWS_LOGSTREAM_INFO(kTag.c_str(), "Benchmark cost: $" + std::to_string(benchmark_cost_) + ".");

  Teardown();

  return benchmark_outputs;
}

void InvocationLatencyBenchmark::Setup() {
  std::vector<Aws::String> s3_package_names;
  std::regex s3_package_name_regex("S3_([^-]*)");

  for (const auto& package_name : kPackageNames) {
    std::smatch matches;
    if (std::regex_search(package_name, matches, s3_package_name_regex)) {
      s3_package_names.emplace_back(matches[1]);
    }

    // TODO(anyone): Increase kOverprovisioningCoefficient if not enough traces are available
    for (const auto function_instance_mb_size : function_instance_mb_sizes_) {
      for (const auto invocation_count : invocation_counts_) {
        for (const auto warm_mode : warm_modes_) {
          for (const auto sleep_ms_duration : sleep_ms_durations_) {
            const auto benchmark_config = std::make_shared<LambdaBenchmarkConfig>(
                LambdaBenchmarkConfig{package_name,
                                      function_instance_mb_size,
                                      repetition_count_,
                                      static_cast<size_t>(invocation_count * kOverprovisioningCoefficient),
                                      warm_mode ? WarmUp::kDefault : WarmUp::kNone,
                                      warm_mode ? UseOneFunctionPerRepetition::kNo : UseOneFunctionPerRepetition::kYes,
                                      UseEventQueue::kNo,
                                      {},
                                      kBenchmarkName,
                                      kEnableTracing});

            const auto payload_stream = std::make_shared<Aws::StringStream>(
                Aws::Utils::Json::JsonValue().WithInt64("sleep_ms", sleep_ms_duration).View().WriteCompact());
            benchmark_config->SetOnePayloadForAllFunctions(payload_stream);

            benchmark_configs_.emplace_back(
                InvocationLatencyBenchmarkParameters{package_name, function_instance_mb_size, invocation_count,
                                                     warm_mode, sleep_ms_duration, repetition_count_},
                benchmark_config);
          }
        }
      }
    }
  }

  helper_->CreateS3BucketIfNotExists(kBenchmarkName);

  std::vector<std::tuple<Aws::String, std::shared_ptr<Aws::IOStream>, size_t>> s3_objects;
  s3_objects.reserve(s3_package_names.size());

  for (const auto& s3_package_name : s3_package_names) {
    std::shared_ptr<Aws::IOStream> package_file = Aws::MakeShared<Aws::FStream>(
        s3_package_name.c_str(), LambdaBenchmarkConfig::GetProjectDirPath() + "pkg/" + s3_package_name + ".zip",
        std::ios_base::in | std::ios_base::binary);

    auto file_size = package_file->tellg();
    package_file->seekg(0, std::ios::end);
    file_size = package_file->tellg() - file_size;
    package_file->seekg(0, std::ios::beg);

    s3_objects.emplace_back(s3_package_name, package_file, file_size);
  }

  helper_->UploadObjectsToS3Parallel(s3_objects, kBenchmarkName);
}

void InvocationLatencyBenchmark::Teardown() {
  benchmark_configs_.clear();
  benchmark_configs_.shrink_to_fit();
  function_segments_analyzer_ = std::make_shared<FunctionSegmentsAnalyzer>(xray_client_);
  benchmark_cost_ = 0;
}

long double InvocationLatencyBenchmark::CalculateBenchmarkCost(
    const std::vector<std::shared_ptr<LambdaBenchmarkResult>>& benchmark_results) {
  long double lambda_cost = 0;
  long double xray_cost = 0;

  for (size_t i = 0; i < benchmark_results.size(); ++i) {
    lambda_cost +=
        CalculateOverallFunctionCost(benchmark_results[i], benchmark_configs_[i].first.function_instance_mb_size);
  }

  xray_cost += cost_calculator_->CalculateCostXray(
      function_instance_mb_sizes_.size() *
          (std::accumulate(invocation_counts_.begin(), invocation_counts_.end(), static_cast<size_t>(0)) *
           kOverprovisioningCoefficient) *
          warm_modes_.size() * repetition_count_,
      function_segments_analyzer_->GetNumScannedTraces(), function_segments_analyzer_->GetNumAccessedTraces());

  return lambda_cost + xray_cost;
}

Aws::Utils::Json::JsonValue InvocationLatencyBenchmark::GenerateResultOutput(
    const std::shared_ptr<LambdaBenchmarkResult>& benchmark_result,
    const InvocationLatencyBenchmarkParameters& benchmark_parameters,
    const std::shared_ptr<std::unordered_map<Aws::String, LambdaSegmentDurations>>& result_segments) const {
  Aws::StringStream benchmark_name;
  benchmark_name << "InvocationLatencyBenchmark/" << benchmark_parameters.function_package_name << "/"
                 << benchmark_parameters.function_instance_mb_size << "/" << benchmark_parameters.invocation_count
                 << "/" << (benchmark_parameters.warm_mode ? "Warm" : "Cold") << "/"
                 << benchmark_parameters.sleep_ms_duration << "/" << benchmark_parameters.repetition_count;

  const auto& segments = FunctionSegmentsAnalyzer::CreateLambdaSegmentDurations();
  std::vector<std::function<std::tuple<Aws::String, double>(const LambdaInvokeResult&)>> extract_metric_functions;
  extract_metric_functions.reserve(segments.size() + 1);

  for (const auto& segment : segments) {
    extract_metric_functions.emplace_back([&segment, &result_segments](const LambdaInvokeResult& invoke_result) {
      return std::make_tuple(
          segment.first,
          std::chrono::duration<double>((*result_segments)[invoke_result.GetInvokeId()][segment.first]).count());
    });
  }

  extract_metric_functions.emplace_back([&](const LambdaInvokeResult& invoke_result) {
    return std::make_tuple("function_cost_usd",
                           ExtractFunctionCost(invoke_result, benchmark_parameters.function_instance_mb_size));
  });

  std::vector<std::tuple<Aws::String, double>> aggregated_metrics;

  for (const auto& segment : segments) {
    std::vector<double> metrics;
    metrics.reserve(result_segments->size());

    std::transform(result_segments->cbegin(), result_segments->cend(), std::back_inserter(metrics),
                   [&segment](const std::pair<Aws::String, LambdaSegmentDurations>& segments) {
                     return std::chrono::duration<double>(segments.second.at(segment.first)).count();
                   });

    if (metrics.empty()) {
      continue;
    }

    const BenchmarkResultAggregate aggregates(metrics);

    aggregated_metrics.emplace_back(segment.first + "_minimum", aggregates.GetMinimum());
    aggregated_metrics.emplace_back(segment.first + "_maximum", aggregates.GetMaximum());
    aggregated_metrics.emplace_back(segment.first + "_median", aggregates.GetMedian());
    aggregated_metrics.emplace_back(segment.first + "_average", aggregates.GetAverage());
    aggregated_metrics.emplace_back(segment.first + "_percentile_25", aggregates.GetPercentile(25));
    aggregated_metrics.emplace_back(segment.first + "_percentile_75", aggregates.GetPercentile(75));
    aggregated_metrics.emplace_back(segment.first + "_percentile_90", aggregates.GetPercentile(90));
    aggregated_metrics.emplace_back(segment.first + "_percentile_99", aggregates.GetPercentile(99));
    aggregated_metrics.emplace_back(segment.first + "_percentile_99_9", aggregates.GetPercentile(99.9));
    aggregated_metrics.emplace_back(segment.first + "_percentile_99_99", aggregates.GetPercentile(99.99));
    aggregated_metrics.emplace_back(segment.first + "_standard_deviation", aggregates.GetStandardDeviation());
  }

  aggregated_metrics.emplace_back("benchmark_cost_usd", static_cast<double>(benchmark_cost_));
  aggregated_metrics.emplace_back("warm_up_cost_usd", static_cast<double>(benchmark_result->GetWarmUpCost()));

  auto json_output = GenerateJsonOutput(benchmark_name.str(), aggregated_metrics, {}, benchmark_result,
                                        extract_metric_functions, {}, {});

  return json_output;
}

}  // namespace skyrise
