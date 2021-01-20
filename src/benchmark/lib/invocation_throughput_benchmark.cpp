#include "invocation_throughput_benchmark.hpp"

#include <algorithm>

#include <magic_enum.hpp>

#include "utils/string.hpp"

namespace skyrise {

InvocationThroughputBenchmark::InvocationThroughputBenchmark(const std::vector<size_t>& function_instance_mb_sizes,
                                                             const std::vector<size_t>& invocation_counts,
                                                             const std::vector<size_t>& function_payload_byte_sizes) {
  benchmark_configs_.reserve(function_instance_mb_sizes.size() * invocation_counts.size() * 2);

  for (const auto function_instance_mb_size : function_instance_mb_sizes) {
    for (const auto invocation_count : invocation_counts) {
      for (const auto use_event_queue : {UseEventQueue::kYes, UseEventQueue::kNo}) {
        for (const auto function_payload_byte_size : function_payload_byte_sizes) {
          // TODO(anyone): Use repetition/invocation framework instead of multiple configs
          BenchmarkConfig config(kFunctionName, function_instance_mb_size, 1, invocation_count,
                                 WarmUpStrategy::kDefault, UseOneFunctionPerRepetition::kNo, use_event_queue);

          if (function_payload_byte_size > 0) {
            config.SetOnePayloadForAllFunctions(BenchmarkHelper::GenerateRandomObject(function_payload_byte_size));
          }

          benchmark_configs_.emplace_back(config);
        }
      }
    }
  }
}

Aws::Utils::Array<Aws::Utils::Json::JsonValue> InvocationThroughputBenchmark::Run(
    const std::shared_ptr<BenchmarkRunner>& benchmark_runner) {
  std::vector<std::shared_ptr<BenchmarkResult>> benchmark_results;
  benchmark_results.reserve(benchmark_configs_.size());

  for (const auto& benchmark_config : benchmark_configs_) {
    benchmark_results.emplace_back(benchmark_runner->RunConfig(benchmark_config));
  }

  Aws::Utils::Array<Aws::Utils::Json::JsonValue> benchmark_outputs(benchmark_results.size());

  for (size_t i = 0; i < benchmark_results.size(); ++i) {
    benchmark_outputs[i] =
        InvocationThroughputBenchmark::GenerateResultOutput(benchmark_results[i], benchmark_configs_[i]);
  }

  return benchmark_outputs;
}

Aws::Utils::Json::JsonValue InvocationThroughputBenchmark::GenerateResultOutput(
    const std::shared_ptr<BenchmarkResult>& benchmark_result, const BenchmarkConfig& benchmark_config) {
  Aws::StringStream benchmark_name;
  benchmark_name << "InvocationThroughputBenchmark/" << benchmark_config.function_configs_.front().memory_size << "/"
                 << benchmark_config.concurrent_invocation_count_ << "/UseEventQueue"
                 << (static_cast<bool>(benchmark_config.use_event_queue_) ? "Yes" : "No") << "/"
                 << StreamToString(benchmark_config.repetition_configs_.front().front().payload.get()).size();

  const auto invocation_results = benchmark_result->GetInvocationResults().front();

  auto min_start_time = std::chrono::steady_clock::time_point::max();
  auto max_end_time = std::chrono::steady_clock::time_point::min();

  for (const auto& item_result : invocation_results) {
    min_start_time = std::min(min_start_time, item_result.second.start_point_);
    max_end_time = std::max(max_end_time, item_result.second.end_point_);
  }

  const double duration = std::chrono::duration<double>(max_end_time - min_start_time).count();
  const double throughput = benchmark_config.concurrent_invocation_count_ / duration;

  return BenchmarkHelper::GenerateJsonOutput(
      benchmark_name.str(), {{"throughput", throughput}}, {/*aggregated string metrics*/}, benchmark_result,
      {[&](const InvocationResult& item_result) {
        return std::make_tuple(
            "duration", std::chrono::duration<double>(item_result.end_point_ - item_result.start_point_).count());
      }},
      {/*extract string metric functions*/});
}

}  // namespace skyrise
