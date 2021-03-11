#include "idle_lifetime_benchmark.hpp"

#include <algorithm>
#include <chrono>
#include <set>
#include <thread>

#include <magic_enum.hpp>

#include "benchmark_result_aggregate.hpp"
#include "utils/assert.hpp"
#include "utils/map.hpp"
#include "utils/string.hpp"

namespace skyrise {

IdleLifetimeBenchmark::IdleLifetimeBenchmark(std::shared_ptr<CostCalculator> cost_calculator,
                                             const std::vector<size_t>& function_instance_mb_sizes,
                                             const std::vector<size_t>& invocation_counts,
                                             const std::vector<size_t>& sleep_min_durations,
                                             const size_t repetition_count)
    : Benchmark(std::move(cost_calculator)) {
  benchmark_configs_.reserve(function_instance_mb_sizes.size() * invocation_counts.size() * sleep_min_durations.size());

  for (const auto sleep_min_duration : sleep_min_durations) {
    std::vector<std::function<void()>> after_repetition_callbacks;
    after_repetition_callbacks.reserve(repetition_count + 1);

    for (size_t i = 0; i < repetition_count; ++i) {
      after_repetition_callbacks.emplace_back(
          [sleep_min_duration]() { std::this_thread::sleep_for(std::chrono::minutes(sleep_min_duration)); });
    }

    after_repetition_callbacks.emplace_back([]() {});

    for (const auto function_instance_mb_size : function_instance_mb_sizes) {
      for (const auto invocation_count : invocation_counts) {
        benchmark_configs_.emplace_back(
            IdleLifetimeBenchmarkParameters{function_instance_mb_size, invocation_count, sleep_min_duration,
                                            repetition_count},
            BenchmarkConfig(kFunctionName, function_instance_mb_size, after_repetition_callbacks.size(),
                            invocation_count, WarmUp::kNone, UseOneFunctionPerRepetition::kNo, UseEventQueue::kNo,
                            after_repetition_callbacks));
      }
    }
  }
}

Aws::Utils::Array<Aws::Utils::Json::JsonValue> IdleLifetimeBenchmark::Run(
    const std::shared_ptr<BenchmarkRunner>& benchmark_runner) {
  std::vector<std::shared_ptr<BenchmarkResult>> benchmark_results;
  benchmark_results.reserve(benchmark_configs_.size());

  for (const auto& benchmark_config : benchmark_configs_) {
    benchmark_results.emplace_back(benchmark_runner->RunConfig(benchmark_config.second));
  }

  Aws::Utils::Array<Aws::Utils::Json::JsonValue> benchmark_outputs(benchmark_results.size());

  for (size_t i = 0; i < benchmark_results.size(); ++i) {
    benchmark_outputs[i] =
        IdleLifetimeBenchmark::GenerateResultOutput(benchmark_results[i], benchmark_configs_[i].first);
  }

  return benchmark_outputs;
}

Aws::Utils::Json::JsonValue IdleLifetimeBenchmark::GenerateResultOutput(
    const std::shared_ptr<BenchmarkResult>& benchmark_result,
    const IdleLifetimeBenchmarkParameters& benchmark_parameters) const {
  Aws::StringStream benchmark_name;
  benchmark_name << "IdleLifetimeBenchmark/" << benchmark_parameters.function_instance_mb_size << "/"
                 << benchmark_parameters.invocation_count << "/" << benchmark_parameters.sleep_min_duration << "/"
                 << benchmark_parameters.repetition_count;

  const auto& invocation_results = benchmark_result->GetInvocationResults();

  std::vector<double> idle_lifetime_percentages;
  idle_lifetime_percentages.reserve(invocation_results.size() - 1);

  std::set<std::string> initial_vm_ids;

  for (size_t i = 0; i < invocation_results.size(); ++i) {
    std::set<std::string> observed_vm_ids;

    for (const auto& invocation : invocation_results[i]) {
      const Aws::String vm_id = StreamToString(&invocation.second.invoke_result->GetPayload());

      if (i == 0) {
        initial_vm_ids.emplace(vm_id);
      } else if (initial_vm_ids.count(vm_id) > 0) {
        observed_vm_ids.emplace(vm_id);
      }
    }

    if (i > 0) {
      idle_lifetime_percentages.emplace_back(observed_vm_ids.size() / static_cast<double>(initial_vm_ids.size()));
    }
  }

  const BenchmarkResultAggregate aggregates(idle_lifetime_percentages);

  return BenchmarkHelper::GenerateJsonOutput(
      benchmark_name.str(),
      {{"idle_lifetime_percentage_minimum", aggregates.GetMinimum()},
       {"idle_lifetime_percentage_maximum", aggregates.GetMaximum()},
       {"idle_lifetime_percentage_average", aggregates.GetAverage()},
       {"idle_lifetime_percentage_median", aggregates.GetMedian()},
       {"idle_lifetime_percentage_percentile_0.01", aggregates.GetPercentile(0.01)},
       {"idle_lifetime_percentage_percentile_0.1", aggregates.GetPercentile(0.1)},
       {"idle_lifetime_percentage_percentile_1", aggregates.GetPercentile(1)},
       {"idle_lifetime_percentage_percentile_10", aggregates.GetPercentile(10)},
       {"idle_lifetime_percentage_std_dev", aggregates.GetStandardDeviation()},
       {"benchmark_cost_usd", static_cast<double>(CalculateOverallFunctionCost(
                                  benchmark_result, benchmark_parameters.function_instance_mb_size))}},
      {/*aggregated string metrics*/}, benchmark_result, {[&](const InvocationResult& item_result) {
        return std::make_tuple("function_cost_usd",
                               ExtractFunctionCost(item_result, benchmark_parameters.function_instance_mb_size));
      }},
      {[&](const InvocationResult& item_result) {
        return std::make_tuple("vm_id", StreamToString(&item_result.invoke_result->GetPayload()));
      }},
      {/*extract object metric functions*/});
}

}  // namespace skyrise
