#include "idle_lifetime_benchmark.hpp"

#include <algorithm>
#include <chrono>
#include <set>
#include <thread>

#include <magic_enum.hpp>

#include "utils/assert.hpp"
#include "utils/map.hpp"
#include "utils/string.hpp"

namespace skyrise {

IdleLifetimeBenchmark::IdleLifetimeBenchmark(const std::vector<size_t>& function_instance_mb_sizes,
                                             const std::vector<size_t>& invocation_counts,
                                             const std::vector<size_t>& sleep_min_durations)
    : sleep_min_durations_(sleep_min_durations) {
  Assert(std::is_sorted(sleep_min_durations.cbegin(), sleep_min_durations.cend()),
         "sleep_min_durations has to be sorted in ascending order.");

  std::vector<std::function<void()>> after_repetition_callbacks;
  after_repetition_callbacks.reserve(sleep_min_durations.size() + 1);

  for (const auto& sleep_min_duration : sleep_min_durations) {
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

Aws::Utils::Array<Aws::Utils::Json::JsonValue> IdleLifetimeBenchmark::Run(
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

Aws::Utils::Json::JsonValue IdleLifetimeBenchmark::GenerateResultOutput(
    const std::shared_ptr<std::vector<BenchmarkItemResult>>& benchmark_result,
    const BenchmarkConfig& benchmark_config) {
  // TODO(maltenbergert): Move this into a CreateBenchmarkName helper when extending the abstract Benchmark class
  Aws::StringStream benchmark_name;
  benchmark_name << "IdleLifetimeBenchmark/" << benchmark_config.function_configs_->front().memory_size << "/"
                 << benchmark_config.num_invocations_ << "/" << VectorToString(sleep_min_durations_, ",");

  std::map<Aws::String, double> vm_ids_to_idle_lifetimes;

  for (size_t i = 0; i < benchmark_config.num_repetitions_; ++i) {
    for (size_t j = 0; j < benchmark_config.num_invocations_; ++j) {
      const Aws::String vm_id =
          StreamToString(&(*benchmark_result)[i * benchmark_config.num_invocations_ + j].invoke_result->GetPayload());

      if (i == 0) {
        vm_ids_to_idle_lifetimes.emplace(vm_id, 0);
      } else if (vm_ids_to_idle_lifetimes.count(vm_id) > 0) {
        vm_ids_to_idle_lifetimes[vm_id] = static_cast<double>(sleep_min_durations_[i - 1]);
      }
    }
  }

  const auto aggregates = BenchmarkHelper::CalculateAggregates(ExtractMapValues(vm_ids_to_idle_lifetimes));

  return BenchmarkHelper::GenerateJsonOutput(
      benchmark_name.str(),
      {{"idle_lifetime_min_minimum", aggregates.minimum},
       {"idle_lifetime_min_maximum", aggregates.maximum},
       {"idle_lifetime_min_average", aggregates.average},
       {"idle_lifetime_min_median", aggregates.median},
       {"idle_lifetime_min_percentile_0.01", aggregates.percentile_0_01},
       {"idle_lifetime_min_percentile_0.1", aggregates.percentile_0_1},
       {"idle_lifetime_min_percentile_1", aggregates.percentile_1},
       {"idle_lifetime_min_percentile_10", aggregates.percentile_10},
       {"idle_lifetime_min_std_dev", aggregates.standard_deviation}},
      {/*aggregated string metrics*/}, benchmark_result, {/*extract double metric functions*/},
      {[&](const BenchmarkItemResult& item_result) {
        return std::make_tuple("vm_id", StreamToString(&item_result.invoke_result->GetPayload()));
      }});
}

}  // namespace skyrise
