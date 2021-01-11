#include "function_colocation_benchmark.hpp"

#include <algorithm>
#include <chrono>
#include <numeric>
#include <set>
#include <thread>

#include <magic_enum.hpp>

#include "benchmark_result_aggregate.hpp"
#include "utils/assert.hpp"
#include "utils/map.hpp"
#include "utils/string.hpp"

namespace skyrise {

FunctionColocationBenchmark::FunctionColocationBenchmark(const std::vector<size_t>& function_instance_mb_sizes,
                                                         const std::vector<size_t>& invocation_counts,
                                                         const size_t sleep_min_duration, const size_t repetition_count)
    : sleep_min_duration_(sleep_min_duration) {
  std::vector<std::function<void()>> after_repetition_callbacks;
  after_repetition_callbacks.reserve(repetition_count + 1);

  for (size_t i = 0; i < repetition_count; ++i) {
    after_repetition_callbacks.emplace_back(
        [sleep_min_duration]() { std::this_thread::sleep_for(std::chrono::minutes(sleep_min_duration)); });
  }

  after_repetition_callbacks.emplace_back([]() {});

  benchmark_configs_.reserve(function_instance_mb_sizes.size() * invocation_counts.size());

  for (const auto& function_instance_mb_size : function_instance_mb_sizes) {
    for (const auto& invocation_count : invocation_counts) {
      benchmark_configs_.emplace_back(kFunctionName, function_instance_mb_size, invocation_count, kExecuteMode,
                                      after_repetition_callbacks.size(), after_repetition_callbacks);
    }
  }
}

Aws::Utils::Array<Aws::Utils::Json::JsonValue> FunctionColocationBenchmark::Run(
    const std::shared_ptr<BenchmarkRunner>& benchmark_runner) {
  std::vector<std::shared_ptr<BenchmarkResult>> benchmark_results;
  benchmark_results.reserve(benchmark_configs_.size());

  for (const auto& benchmark_config : benchmark_configs_) {
    benchmark_results.emplace_back(benchmark_runner->RunConfig(benchmark_config));
  }

  Aws::Utils::Array<Aws::Utils::Json::JsonValue> benchmark_outputs(benchmark_results.size());

  for (size_t i = 0; i < benchmark_results.size(); ++i) {
    benchmark_outputs[i] = GenerateResultOutput(benchmark_results[i], benchmark_configs_[i]);
  }

  return benchmark_outputs;
}

Aws::Utils::Json::JsonValue FunctionColocationBenchmark::GenerateResultOutput(
    const std::shared_ptr<BenchmarkResult>& benchmark_result, const BenchmarkConfig& benchmark_config) const {
  Aws::StringStream benchmark_name;
  benchmark_name << "ColocationBenchmark/" << benchmark_config.function_configs_->front().memory_size << "/"
                 << benchmark_config.invocation_count_ << "/" << sleep_min_duration_ << "/"
                 << (benchmark_config.repetition_count_ - 1);

  const auto& benchmark_item_results = benchmark_result->GetInvocationResults();

  std::vector<double> colocation_counts;

  for (size_t i = 0; i < benchmark_config.repetition_count_; ++i) {
    std::map<std::string, size_t> vm_ids_to_colocation_counts;

    for (const auto& benchmark_item_result : benchmark_item_results[i]) {
      const std::string vm_id = StreamToString(&benchmark_item_result.second.invoke_result_->GetPayload());

      if (vm_ids_to_colocation_counts.count(vm_id) == 0) {
        vm_ids_to_colocation_counts.emplace(vm_id, 1);
      } else {
        ++vm_ids_to_colocation_counts[vm_id];
      }
    }

    for (const auto& [vm_id, colocation_count] : vm_ids_to_colocation_counts) {
      colocation_counts.emplace_back(static_cast<double>(colocation_count));
    }
  }

  const BenchmarkResultAggregate aggregates(colocation_counts);

  return BenchmarkHelper::GenerateJsonOutput(
      benchmark_name.str(),
      {{"colocation_counts_minimum", aggregates.GetMinimum()},
       {"colocation_counts_maximum", aggregates.GetMaximum()},
       {"colocation_counts_average", aggregates.GetAverage()},
       {"colocation_counts_median", aggregates.GetMedian()},
       {"colocation_counts_90", aggregates.GetPercentile(90)},
       {"colocation_counts_99", aggregates.GetPercentile(99)},
       {"colocation_counts_99.9", aggregates.GetPercentile(99.9)},
       {"colocation_counts_99.99", aggregates.GetPercentile(99.99)},
       {"colocation_counts_std_dev", aggregates.GetStandardDeviation()}},
      {/*aggregated string metrics*/}, benchmark_result, {/*extract double metric functions*/},
      {[&](const InvocationResult& item_result) {
        return std::make_tuple("vm_id", StreamToString(&item_result.invoke_result_->GetPayload()));
      }});
}

}  // namespace skyrise
