#include "idle_availability_benchmark.hpp"

#include <algorithm>
#include <chrono>
#include <numeric>
#include <set>
#include <thread>

#include <magic_enum.hpp>

#include "utils/assert.hpp"
#include "utils/map.hpp"
#include "utils/string.hpp"

namespace skyrise {

IdleAvailabilityBenchmark::IdleAvailabilityBenchmark(const std::vector<size_t>& function_instance_mb_sizes,
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

Aws::Utils::Array<Aws::Utils::Json::JsonValue> IdleAvailabilityBenchmark::Run(
    const std::shared_ptr<BenchmarkRunner>& benchmark_runner) {
  std::vector<std::shared_ptr<std::vector<BenchmarkItemResult>>> benchmark_results;
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

Aws::Utils::Json::JsonValue IdleAvailabilityBenchmark::GenerateResultOutput(
    const std::shared_ptr<std::vector<BenchmarkItemResult>>& benchmark_result,
    const BenchmarkConfig& benchmark_config) const {
  Aws::StringStream benchmark_name;
  benchmark_name << "IdleAvailabilityBenchmark/" << benchmark_config.function_configs_->front().memory_size << "/"
                 << benchmark_config.invocation_count_ << "/" << sleep_min_duration_ << "/"
                 << benchmark_config.repetition_count_;

  std::map<std::string, std::vector<bool>> vm_ids_to_availability_flags;

  for (size_t i = 0; i < benchmark_config.repetition_count_; ++i) {
    for (size_t j = 0; j < benchmark_config.invocation_count_; ++j) {
      const std::string vm_id =
          StreamToString(&(*benchmark_result)[i * benchmark_config.invocation_count_ + j].invoke_result->GetPayload());

      if (i == 0) {
        vm_ids_to_availability_flags.try_emplace(vm_id, benchmark_config.repetition_count_, false);
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
                                          static_cast<double>(benchmark_config.repetition_count_));
    unavailable_phases_counts.emplace_back(static_cast<double>(unavailable_phases_count));
  }

  const auto availability_percentages_aggregates = BenchmarkHelper::CalculateAggregates(availability_percentages);
  const auto unavailable_phases_counts_aggregates = BenchmarkHelper::CalculateAggregates(unavailable_phases_counts);
  const auto unavailable_phases_lengths_aggregates = BenchmarkHelper::CalculateAggregates(unavailable_phases_lengths);

  return BenchmarkHelper::GenerateJsonOutput(
      benchmark_name.str(),
      {{"availability_percentage_minimum", availability_percentages_aggregates.minimum},
       {"availability_percentage_maximum", availability_percentages_aggregates.maximum},
       {"availability_percentage_average", availability_percentages_aggregates.average},
       {"availability_percentage_median", availability_percentages_aggregates.median},
       {"availability_percentage_percentile_0.01", availability_percentages_aggregates.percentile_0_01},
       {"availability_percentage_percentile_0.1", availability_percentages_aggregates.percentile_0_1},
       {"availability_percentage_percentile_1", availability_percentages_aggregates.percentile_1},
       {"availability_percentage_percentile_10", availability_percentages_aggregates.percentile_10},
       {"availability_percentage_std_dev", availability_percentages_aggregates.standard_deviation},
       {"unavailable_phases_count_minimum", unavailable_phases_counts_aggregates.minimum},
       {"unavailable_phases_count_maximum", unavailable_phases_counts_aggregates.maximum},
       {"unavailable_phases_count_average", unavailable_phases_counts_aggregates.average},
       {"unavailable_phases_count_median", unavailable_phases_counts_aggregates.median},
       {"unavailable_phases_count_percentile_90", unavailable_phases_counts_aggregates.percentile_90},
       {"unavailable_phases_count_percentile_99", unavailable_phases_counts_aggregates.percentile_99},
       {"unavailable_phases_count_percentile_99.9", unavailable_phases_counts_aggregates.percentile_99_9},
       {"unavailable_phases_count_percentile_99.99", unavailable_phases_counts_aggregates.percentile_99_99},
       {"unavailable_phases_count_std_dev", unavailable_phases_counts_aggregates.standard_deviation},
       {"unavailable_phases_length_minimum", unavailable_phases_lengths_aggregates.minimum},
       {"unavailable_phases_length_maximum", unavailable_phases_lengths_aggregates.maximum},
       {"unavailable_phases_length_average", unavailable_phases_lengths_aggregates.average},
       {"unavailable_phases_length_median", unavailable_phases_lengths_aggregates.median},
       {"unavailable_phases_length_percentile_90", unavailable_phases_lengths_aggregates.percentile_90},
       {"unavailable_phases_length_percentile_99", unavailable_phases_lengths_aggregates.percentile_99},
       {"unavailable_phases_length_percentile_99.9", unavailable_phases_lengths_aggregates.percentile_99_9},
       {"unavailable_phases_length_percentile_99.99", unavailable_phases_lengths_aggregates.percentile_99_99},
       {"unavailable_phases_length_std_dev", unavailable_phases_lengths_aggregates.standard_deviation}},
      {/*aggregated string metrics*/}, benchmark_result, {/*extract double metric functions*/},
      {[&](const BenchmarkItemResult& item_result) {
        return std::make_tuple("vm_id", StreamToString(&item_result.invoke_result->GetPayload()));
      }});
}

}  // namespace skyrise
