#include "idle_availability_benchmark.hpp"

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

IdleAvailabilityBenchmark::IdleAvailabilityBenchmark(std::shared_ptr<const CostCalculator> cost_calculator,
                                                     const std::vector<size_t>& function_instance_mb_sizes,
                                                     const std::vector<size_t>& invocation_counts,
                                                     const std::vector<size_t>& sleep_min_durations,
                                                     const size_t repetition_count)
    : LambdaBenchmark(std::move(cost_calculator)) {
  benchmark_configs_.reserve(function_instance_mb_sizes.size() * invocation_counts.size() * sleep_min_durations.size());

  for (const auto sleep_min_duration : sleep_min_durations) {
    std::vector<std::function<void()>> after_repetition_callbacks;
    after_repetition_callbacks.reserve(repetition_count + 1);

    for (size_t i = 0; i < repetition_count; ++i) {
      after_repetition_callbacks.emplace_back(
          [sleep_min_duration]() { std::this_thread::sleep_for(std::chrono::minutes(sleep_min_duration)); });
    }

    after_repetition_callbacks.emplace_back([]() {});

    benchmark_configs_.reserve(function_instance_mb_sizes.size() * invocation_counts.size());

    for (const auto function_instance_mb_size : function_instance_mb_sizes) {
      for (const auto invocation_count : invocation_counts) {
        benchmark_configs_.emplace_back(
            IdleAvailabilityBenchmarkParameters{function_instance_mb_size, invocation_count, sleep_min_duration,
                                                repetition_count},
            std::make_shared<LambdaBenchmarkConfig>(
                kFunctionName, function_instance_mb_size, after_repetition_callbacks.size(), invocation_count,
                WarmUp::kNone, UseOneFunctionPerRepetition::kNo, UseEventQueue::kNo, after_repetition_callbacks));
      }
    }
  }
}

Aws::Utils::Array<Aws::Utils::Json::JsonValue> IdleAvailabilityBenchmark::OnRun(
    const std::shared_ptr<LambdaBenchmarkRunner>& benchmark_runner) {
  std::vector<std::shared_ptr<LambdaBenchmarkResult>> benchmark_results;
  benchmark_results.reserve(benchmark_configs_.size());

  for (const auto& benchmark_config : benchmark_configs_) {
    benchmark_results.emplace_back(benchmark_runner->RunLambdaConfig(benchmark_config.second));
  }

  Aws::Utils::Array<Aws::Utils::Json::JsonValue> benchmark_outputs(benchmark_results.size());

  for (size_t i = 0; i < benchmark_results.size(); ++i) {
    benchmark_outputs[i] =
        IdleAvailabilityBenchmark::GenerateResultOutput(benchmark_results[i], benchmark_configs_[i].first);
  }

  return benchmark_outputs;
}

Aws::Utils::Json::JsonValue IdleAvailabilityBenchmark::GenerateResultOutput(
    const std::shared_ptr<LambdaBenchmarkResult>& benchmark_result,
    const IdleAvailabilityBenchmarkParameters& benchmark_parameters) const {
  Aws::StringStream benchmark_name;
  benchmark_name << "IdleAvailabilityBenchmark/" << benchmark_parameters.function_instance_mb_size << "/"
                 << benchmark_parameters.invocation_count << "/" << benchmark_parameters.sleep_min_duration << "/"
                 << benchmark_parameters.repetition_count;

  const auto& benchmark_repetitions = benchmark_result->GetBenchmarkRepetitions();

  std::map<std::string, std::vector<bool>> vm_ids_to_availability_flags;

  for (size_t i = 0; i < benchmark_repetitions.size(); ++i) {
    for (const auto& invoke_result : benchmark_repetitions[i].GetInvokeResults()) {
      const std::string vm_id = invoke_result.GetResponseBody().AsString();

      if (i == 0) {
        vm_ids_to_availability_flags.try_emplace(vm_id, benchmark_parameters.repetition_count, false);
      } else if (vm_ids_to_availability_flags.count(vm_id) > 0) {
        vm_ids_to_availability_flags[vm_id][i] = true;
      }
    }
  }

  std::vector<double> availability_percentages;
  availability_percentages.reserve(vm_ids_to_availability_flags.size());

  std::vector<double> unavailable_phases_counts;
  unavailable_phases_counts.reserve(vm_ids_to_availability_flags.size());

  std::vector<double> unavailable_phases_lengths;
  unavailable_phases_lengths.reserve(vm_ids_to_availability_flags.size());

  for (const auto& [vm_id, availability_flags] : vm_ids_to_availability_flags) {
    bool is_available = true;

    size_t available_repetition_count = 0;
    size_t unavailable_phases_count = 0;
    size_t unavailable_phases_length = 0;

    for (size_t i = 0; i < availability_flags.size(); ++i) {
      if (availability_flags[i]) {
        ++unavailable_phases_length;

        if (is_available) {
          ++unavailable_phases_count;
          is_available = false;
        }

        if (i == availability_flags.size() - 1) {
          unavailable_phases_lengths.emplace_back(static_cast<double>(unavailable_phases_length));
        }
      } else {
        ++available_repetition_count;

        if (!is_available) {
          unavailable_phases_lengths.emplace_back(static_cast<double>(unavailable_phases_length));
          unavailable_phases_length = 0;
          is_available = true;
        }
      }
    }

    availability_percentages.emplace_back(available_repetition_count /
                                          static_cast<double>(benchmark_parameters.repetition_count));
    unavailable_phases_counts.emplace_back(static_cast<double>(unavailable_phases_count));
  }

  if (unavailable_phases_lengths.empty()) {
    unavailable_phases_lengths.emplace_back(0);
  }

  const BenchmarkResultAggregate availability_percentages_aggregates(availability_percentages);
  const BenchmarkResultAggregate unavailable_phases_counts_aggregates(unavailable_phases_counts);
  const BenchmarkResultAggregate unavailable_phases_lengths_aggregates(unavailable_phases_lengths);

  return GenerateJsonOutput(
      benchmark_name.str(),
      {{"availability_percentage_minimum", availability_percentages_aggregates.GetMinimum()},
       {"availability_percentage_maximum", availability_percentages_aggregates.GetMaximum()},
       {"availability_percentage_average", availability_percentages_aggregates.GetAverage()},
       {"availability_percentage_median", availability_percentages_aggregates.GetMedian()},
       {"availability_percentage_percentile_0.01", availability_percentages_aggregates.GetPercentile(0.01)},
       {"availability_percentage_percentile_0.1", availability_percentages_aggregates.GetPercentile(0.1)},
       {"availability_percentage_percentile_1", availability_percentages_aggregates.GetPercentile(1)},
       {"availability_percentage_percentile_10", availability_percentages_aggregates.GetPercentile(10)},
       {"availability_percentage_std_dev", availability_percentages_aggregates.GetStandardDeviation()},
       {"unavailable_phases_count_minimum", unavailable_phases_counts_aggregates.GetMinimum()},
       {"unavailable_phases_count_maximum", unavailable_phases_counts_aggregates.GetMaximum()},
       {"unavailable_phases_count_average", unavailable_phases_counts_aggregates.GetAverage()},
       {"unavailable_phases_count_median", unavailable_phases_counts_aggregates.GetMedian()},
       {"unavailable_phases_count_percentile_90", unavailable_phases_counts_aggregates.GetPercentile(90)},
       {"unavailable_phases_count_percentile_99", unavailable_phases_counts_aggregates.GetPercentile(99)},
       {"unavailable_phases_count_percentile_99.9", unavailable_phases_counts_aggregates.GetPercentile(99.9)},
       {"unavailable_phases_count_percentile_99.99", unavailable_phases_counts_aggregates.GetPercentile(99.99)},
       {"unavailable_phases_count_std_dev", unavailable_phases_counts_aggregates.GetStandardDeviation()},
       {"unavailable_phases_length_minimum", unavailable_phases_lengths_aggregates.GetMinimum()},
       {"unavailable_phases_length_maximum", unavailable_phases_lengths_aggregates.GetMaximum()},
       {"unavailable_phases_length_average", unavailable_phases_lengths_aggregates.GetAverage()},
       {"unavailable_phases_length_median", unavailable_phases_lengths_aggregates.GetMedian()},
       {"unavailable_phases_length_percentile_90", unavailable_phases_lengths_aggregates.GetPercentile(90)},
       {"unavailable_phases_length_percentile_99", unavailable_phases_lengths_aggregates.GetPercentile(99)},
       {"unavailable_phases_length_percentile_99.9", unavailable_phases_lengths_aggregates.GetPercentile(99.9)},
       {"unavailable_phases_length_percentile_99.99", unavailable_phases_lengths_aggregates.GetPercentile(99.99)},
       {"unavailable_phases_length_std_dev", unavailable_phases_lengths_aggregates.GetStandardDeviation()},
       {"benchmark_cost_usd", static_cast<double>(CalculateOverallFunctionCost(
                                  benchmark_result, benchmark_parameters.function_instance_mb_size))}},
      {/*aggregated string metrics*/}, benchmark_result, {[&](const LambdaInvokeResult& invoke_result) {
        return std::make_tuple("function_cost_usd",
                               ExtractFunctionCost(invoke_result, benchmark_parameters.function_instance_mb_size));
      }},
      {[&](const LambdaInvokeResult& invoke_result) {
        return std::make_tuple("vm_id", invoke_result.GetResponseBody().AsString());
      }},
      {/*extract object metric functions*/});
}

}  // namespace skyrise
