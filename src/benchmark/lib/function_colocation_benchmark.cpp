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

FunctionColocationBenchmark::FunctionColocationBenchmark(std::shared_ptr<CostCalculator> cost_calculator,
                                                         const std::vector<size_t>& function_instance_mb_sizes,
                                                         const std::vector<size_t>& invocation_counts,
                                                         const std::vector<size_t>& sleep_min_durations,
                                                         const size_t repetition_count)
    : Benchmark(std::move(cost_calculator)) {
  benchmark_configs_.reserve(function_instance_mb_sizes.size() * invocation_counts.size() * sleep_min_durations.size());

  for (const auto sleep_min_duration : sleep_min_durations) {
    std::vector<std::function<void()>> after_repetition_callbacks;
    after_repetition_callbacks.reserve(repetition_count - 1);

    for (size_t i = 0; i < repetition_count; ++i) {
      after_repetition_callbacks.emplace_back(
          [sleep_min_duration]() { std::this_thread::sleep_for(std::chrono::minutes(sleep_min_duration)); });
    }

    after_repetition_callbacks.emplace_back([]() {});

    for (const auto function_instance_mb_size : function_instance_mb_sizes) {
      for (const auto invocation_count : invocation_counts) {
        benchmark_configs_.emplace_back(
            FunctionColocationBenchmarkParameters{function_instance_mb_size, invocation_count, sleep_min_duration,
                                                  repetition_count},
            BenchmarkConfig(kFunctionName, function_instance_mb_size, after_repetition_callbacks.size(),
                            invocation_count, WarmUp::kNone, UseOneFunctionPerRepetition::kNo, UseEventQueue::kNo,
                            after_repetition_callbacks));
      }
    }
  }
}

Aws::Utils::Array<Aws::Utils::Json::JsonValue> FunctionColocationBenchmark::Run(
    const std::shared_ptr<BenchmarkRunner>& benchmark_runner) {
  std::vector<std::shared_ptr<BenchmarkResult>> benchmark_results;
  benchmark_results.reserve(benchmark_configs_.size());

  for (const auto& benchmark_config : benchmark_configs_) {
    benchmark_results.emplace_back(benchmark_runner->RunConfig(benchmark_config.second));
  }

  Aws::Utils::Array<Aws::Utils::Json::JsonValue> benchmark_outputs(benchmark_results.size());

  for (size_t i = 0; i < benchmark_results.size(); ++i) {
    benchmark_outputs[i] =
        FunctionColocationBenchmark::GenerateResultOutput(benchmark_results[i], benchmark_configs_[i].first);
  }

  return benchmark_outputs;
}

Aws::Utils::Json::JsonValue FunctionColocationBenchmark::GenerateResultOutput(
    const std::shared_ptr<BenchmarkResult>& benchmark_result,
    const FunctionColocationBenchmarkParameters& benchmark_parameters) const {
  Aws::StringStream benchmark_name;
  benchmark_name << "ColocationBenchmark/" << benchmark_parameters.function_instance_mb_size << "/"
                 << benchmark_parameters.invocation_count << "/" << benchmark_parameters.sleep_min_duration << "/"
                 << benchmark_parameters.repetition_count;

  const auto& benchmark_repetitions = benchmark_result->GetBenchmarkRepetitions();

  std::vector<double> colocation_counts;

  for (const auto& benchmark_repetition : benchmark_repetitions) {
    std::map<std::string, size_t> vm_ids_to_colocation_counts;

    for (const auto& invoke_result : benchmark_repetition.GetInvokeResults()) {
      const std::string vm_id = invoke_result.GetResponseBody().AsString();

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

  return GenerateJsonOutput(
      benchmark_name.str(),
      {{"colocation_counts_minimum", aggregates.GetMinimum()},
       {"colocation_counts_maximum", aggregates.GetMaximum()},
       {"colocation_counts_average", aggregates.GetAverage()},
       {"colocation_counts_median", aggregates.GetMedian()},
       {"colocation_counts_90", aggregates.GetPercentile(90)},
       {"colocation_counts_99", aggregates.GetPercentile(99)},
       {"colocation_counts_99.9", aggregates.GetPercentile(99.9)},
       {"colocation_counts_99.99", aggregates.GetPercentile(99.99)},
       {"colocation_counts_std_dev", aggregates.GetStandardDeviation()},
       {"benchmark_cost_usd", static_cast<double>(CalculateOverallFunctionCost(
                                  benchmark_result, benchmark_parameters.function_instance_mb_size))}},
      {/*aggregated string metrics*/}, benchmark_result, {[&](const InvokeResult& invoke_result) {
        return std::make_tuple("function_cost_usd",
                               ExtractFunctionCost(invoke_result, benchmark_parameters.function_instance_mb_size));
      }},
      {[&](const InvokeResult& invoke_result) {
        return std::make_tuple("vm_id", invoke_result.GetResponseBody().AsString());
      }},
      {/*extract object metric functions*/});
}

}  // namespace skyrise
