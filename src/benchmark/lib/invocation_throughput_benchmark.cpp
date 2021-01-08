#include "invocation_throughput_benchmark.hpp"

#include <algorithm>

#include <magic_enum.hpp>

#include "utils/string.hpp"

namespace skyrise {

InvocationThroughputBenchmark::InvocationThroughputBenchmark(const std::vector<size_t>& function_instance_mb_sizes,
                                                             const std::vector<size_t>& invocation_counts,
                                                             const std::vector<ExecuteMode>& execute_modes,
                                                             const std::vector<size_t>& function_payload_byte_sizes) {
  benchmark_configs_.reserve(function_instance_mb_sizes.size() * invocation_counts.size() * execute_modes.size());

  for (const auto& function_instance_mb_size : function_instance_mb_sizes) {
    for (const auto& invocation_count : invocation_counts) {
      for (const auto& execute_mode : execute_modes) {
        for (const auto& function_payload_byte_size : function_payload_byte_sizes) {
          BenchmarkConfig config(kFunctionName, function_instance_mb_size, invocation_count, execute_mode);

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
  std::vector<std::shared_ptr<std::vector<BenchmarkItemResult>>> benchmark_results;
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
    const std::shared_ptr<std::vector<BenchmarkItemResult>>& benchmark_result,
    const BenchmarkConfig& benchmark_config) {
  Aws::StringStream benchmark_name;
  benchmark_name << "InvocationThroughputBenchmark/" << benchmark_config.function_configs_->front().memory_size << "/"
                 << benchmark_config.invocation_count_ << "/" << magic_enum::enum_name(benchmark_config.execute_mode_)
                 << "/" << StreamToString(benchmark_config.invocation_configs_->front().payload.get()).size();

  auto min_start_time = benchmark_result->front().start_time;
  auto max_end_time = benchmark_result->front().end_time;

  for (const auto& item_result : *benchmark_result) {
    min_start_time = std::min(min_start_time, item_result.start_time);
    max_end_time = std::max(max_end_time, item_result.end_time);
  }

  const double duration = std::chrono::duration<double>(max_end_time - min_start_time).count();
  const double throughput = benchmark_config.invocation_count_ / duration;

  return BenchmarkHelper::GenerateJsonOutput(
      benchmark_name.str(), {{"throughput", throughput}}, {/*aggregated string metrics*/}, benchmark_result,
      {[&](const BenchmarkItemResult& item_result) {
        return std::make_tuple("duration",
                               std::chrono::duration<double>(item_result.end_time - item_result.start_time).count());
      }},
      {/*extract string metric functions*/});
}

}  // namespace skyrise
