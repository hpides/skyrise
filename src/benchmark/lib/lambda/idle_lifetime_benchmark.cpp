#include "idle_lifetime_benchmark.hpp"

#include <algorithm>
#include <chrono>
#include <set>
#include <thread>

#include <magic_enum.hpp>

#include "benchmark_result_aggregate.hpp"
#include "lambda_benchmark_output.hpp"
#include "utils/assert.hpp"
#include "utils/map.hpp"
#include "utils/string.hpp"

namespace skyrise {

IdleLifetimeBenchmark::IdleLifetimeBenchmark(std::shared_ptr<const CostCalculator> cost_calculator,
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

    for (const auto function_instance_mb_size : function_instance_mb_sizes) {
      for (const auto invocation_count : invocation_counts) {
        benchmark_configs_.emplace_back(
            IdleLifetimeBenchmarkParameters{function_instance_mb_size, invocation_count, sleep_min_duration,
                                            repetition_count},
            std::make_shared<LambdaBenchmarkConfig>(
                kFunctionName, function_instance_mb_size, after_repetition_callbacks.size(), invocation_count,
                WarmUp::kNone, UseOneFunctionPerRepetition::kNo, UseEventQueue::kNo, after_repetition_callbacks));
      }
    }
  }
}

Aws::Utils::Array<Aws::Utils::Json::JsonValue> IdleLifetimeBenchmark::OnRun(
    const std::shared_ptr<LambdaBenchmarkRunner>& benchmark_runner) {
  std::vector<std::shared_ptr<LambdaBenchmarkResult>> benchmark_results;
  benchmark_results.reserve(benchmark_configs_.size());

  for (const auto& benchmark_config : benchmark_configs_) {
    benchmark_results.emplace_back(benchmark_runner->RunLambdaConfig(benchmark_config.second));
  }

  Aws::Utils::Array<Aws::Utils::Json::JsonValue> benchmark_outputs(benchmark_results.size());

  for (size_t i = 0; i < benchmark_results.size(); ++i) {
    benchmark_outputs[i] =
        IdleLifetimeBenchmark::GenerateResultOutput(benchmark_results[i], benchmark_configs_[i].first);
  }

  return benchmark_outputs;
}

Aws::Utils::Json::JsonValue IdleLifetimeBenchmark::GenerateResultOutput(
    const std::shared_ptr<LambdaBenchmarkResult>& benchmark_result,
    const IdleLifetimeBenchmarkParameters& benchmark_parameters) const {
  const auto& benchmark_repetitions = benchmark_result->GetBenchmarkRepetitions();

  std::vector<double> idle_lifetime_percentages;
  idle_lifetime_percentages.reserve(benchmark_repetitions.size() - 1);

  std::set<std::string> initial_vm_ids;

  for (size_t i = 0; i < benchmark_repetitions.size(); ++i) {
    std::set<std::string> observed_vm_ids;

    for (const auto& invoke_result : benchmark_repetitions[i].GetInvokeResults()) {
      const Aws::String vm_id = invoke_result.GetResponseBody().AsString();

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

  const BenchmarkResultAggregate aggregate(idle_lifetime_percentages);

  return LambdaBenchmarkOutput("idle_lifetime_benchmark", benchmark_result)
      .WithInt64Argument("function_instance_mb_size", benchmark_parameters.function_instance_mb_size)
      .WithInt64Argument("invocation_count", benchmark_parameters.invocation_count)
      .WithInt64Argument("sleep_min_duration", benchmark_parameters.sleep_min_duration)
      .WithInt64Argument("repetition_count", benchmark_parameters.repetition_count)
      .WithDoubleMetric("idle_lifetime_percentage_minimum", aggregate.GetMinimum())
      .WithDoubleMetric("idle_lifetime_percentage_maximum", aggregate.GetMaximum())
      .WithDoubleMetric("idle_lifetime_percentage_average", aggregate.GetAverage())
      .WithDoubleMetric("idle_lifetime_percentage_median", aggregate.GetMedian())
      .WithDoubleMetric("idle_lifetime_percentage_percentile_0.01", aggregate.GetPercentile(0.01))
      .WithDoubleMetric("idle_lifetime_percentage_percentile_0.1", aggregate.GetPercentile(0.1))
      .WithDoubleMetric("idle_lifetime_percentage_percentile_1", aggregate.GetPercentile(1))
      .WithDoubleMetric("idle_lifetime_percentage_percentile_10", aggregate.GetPercentile(10))
      .WithDoubleMetric("idle_lifetime_percentage_std_dev", aggregate.GetStandardDeviation())
      .WithDoubleMetric("benchmark_cost_usd", static_cast<double>(CalculateOverallFunctionCost(
                                                  benchmark_result, benchmark_parameters.function_instance_mb_size)))
      .WithDoubleInvocationMetric([&](const LambdaInvokeResult& invoke_result) {
        return std::make_tuple("function_cost_usd",
                               ExtractFunctionCost(invoke_result, benchmark_parameters.function_instance_mb_size));
      })
      .WithStringInvocationMetric([&](const LambdaInvokeResult& invoke_result) {
        return std::make_tuple("vm_id", invoke_result.GetResponseBody().AsString());
      })
      .Build();
}

}  // namespace skyrise
