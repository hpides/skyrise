#include "function_warm_up_benchmark.hpp"

#include <algorithm>
#include <chrono>
#include <functional>
#include <thread>

#include "benchmark_helper.hpp"
#include "benchmark_result_aggregate.hpp"
#include "client/client.hpp"

namespace skyrise {

FunctionWarmUpBenchmark::FunctionWarmUpBenchmark(std::shared_ptr<CostCalculator> cost_calculator,
                                                 const std::vector<size_t>& function_instance_mb_sizes,
                                                 const std::vector<size_t>& invocation_counts,
                                                 const std::vector<size_t>& sleep_ms_durations,
                                                 const std::vector<double>& provisioning_factors,
                                                 const size_t repetition_count)
    : Benchmark(std::move(cost_calculator)) {
  benchmark_configs_.reserve(function_instance_mb_sizes.size() * invocation_counts.size() *
                             (1 + sleep_ms_durations.size() * provisioning_factors.size()));

  const auto payload_value =
      Aws::Utils::Json::JsonValue().WithBool("warmup", true).WithInteger("sleep_ms", kFunctionSleepMs);

  for (const auto function_instance_mb_size : function_instance_mb_sizes) {
    for (const auto invocation_count : invocation_counts) {
      for (const auto sleep_ms_duration : sleep_ms_durations) {
        for (const auto provisioning_factor : provisioning_factors) {
          std::vector<std::function<void()>> after_repetition_callbacks(
              repetition_count, [&]() { std::this_thread::sleep_for(std::chrono::minutes(kRepetitionSleepMin)); });

          BenchmarkConfig config(kFunctionName, function_instance_mb_size, repetition_count, invocation_count,
                                 WarmUp::kDefaultOncePerRepetition, UseOneFunctionPerRepetition::kYes,
                                 UseEventQueue::kNo, after_repetition_callbacks);
          config.warm_up_strategy_ =
              std::make_shared<ConfigurableWarmUpStrategy>(false, sleep_ms_duration, provisioning_factor);

          config.SetOnePayloadForAllFunctions(std::make_shared<Aws::StringStream>(payload_value.View().WriteCompact()));

          benchmark_configs_.emplace_back(
              FunctionWarmUpBenchmarkParameters{function_instance_mb_size, invocation_count, repetition_count,
                                                sleep_ms_duration, provisioning_factor,
                                                config.warm_up_strategy_->GetName()},
              config);
        }
      }

      BenchmarkConfig config(kFunctionName, function_instance_mb_size, repetition_count, invocation_count,
                             WarmUp::kDefaultOncePerRepetition, UseOneFunctionPerRepetition::kYes);
      config.warm_up_strategy_ = std::make_shared<ProvisionedConcurrencyWarmUpStrategy>();

      config.SetOnePayloadForAllFunctions(std::make_shared<Aws::StringStream>(payload_value.View().WriteCompact()));

      benchmark_configs_.emplace_back(
          FunctionWarmUpBenchmarkParameters{function_instance_mb_size, invocation_count, repetition_count, 0, 1.0,
                                            config.warm_up_strategy_->GetName()},
          config);
    }
  }
}

Aws::Utils::Array<Aws::Utils::Json::JsonValue> FunctionWarmUpBenchmark::Run(
    const std::shared_ptr<BenchmarkRunner>& benchmark_runner) {
  std::vector<std::shared_ptr<BenchmarkResult>> benchmark_results;

  for (const auto& config : benchmark_configs_) {
    benchmark_results.emplace_back(benchmark_runner->RunConfig(config.second));
  }

  Aws::Utils::Array<Aws::Utils::Json::JsonValue> benchmark_outputs(benchmark_results.size());

  for (size_t i = 0; i < benchmark_results.size(); ++i) {
    benchmark_outputs[i] = GenerateResultOutput(benchmark_results[i], benchmark_configs_[i].first);
  }

  return benchmark_outputs;
}

Aws::Utils::Json::JsonValue FunctionWarmUpBenchmark::GenerateResultOutput(
    const std::shared_ptr<BenchmarkResult>& benchmark_result,
    const FunctionWarmUpBenchmarkParameters& benchmark_parameters) const {
  Aws::StringStream benchmark_name;
  benchmark_name << "FunctionWarmUpBenchmark/" << benchmark_parameters.function_instance_mb_size << "/"
                 << benchmark_parameters.invocation_count << "/" << benchmark_parameters.sleep_ms_duration << "/"
                 << benchmark_parameters.provisioning_factor << "/" << benchmark_parameters.warm_up_strategy;

  const auto is_warm_function = [&](const InvocationResult& invocation_result) {
    return !BenchmarkHelper::ExtractLogResultMetric(invocation_result, "Init Duration") ||
           benchmark_parameters.warm_up_strategy == "ProvisionedConcurrencyWarmUpStrategy";
  };

  const auto& invocation_results = benchmark_result->GetInvocationResults();

  std::vector<double> warm_function_percentages;
  warm_function_percentages.reserve(benchmark_parameters.repetition_count);

  for (const auto& repetition_results : invocation_results) {
    size_t successful_function_count = 0;
    size_t warm_function_count = 0;

    for (const auto& invocation_result : repetition_results) {
      const auto& result = invocation_result.second;

      if (result.invoke_result && result.invoke_result->GetFunctionError().empty()) {
        ++successful_function_count;

        if (is_warm_function(result)) {
          ++warm_function_count;
        }
      }
    }

    warm_function_percentages.emplace_back(warm_function_count / static_cast<double>(successful_function_count));
  }

  const BenchmarkResultAggregate warm_function_percentages_aggregates(warm_function_percentages);

  std::vector<double> function_warm_up_cost(benchmark_result->GetFunctionWarmUpCosts().cbegin(),
                                            benchmark_result->GetFunctionWarmUpCosts().cend());

  const BenchmarkResultAggregate warm_up_cost_aggregates(function_warm_up_cost);

  return BenchmarkHelper::GenerateJsonOutput(
      benchmark_name.str(),
      {{"warm_function_percentage_minimum", warm_function_percentages_aggregates.GetMinimum()},
       {"warm_function_percentage_maximum", warm_function_percentages_aggregates.GetMaximum()},
       {"warm_function_percentage_average", warm_function_percentages_aggregates.GetAverage()},
       {"warm_function_percentage_median", warm_function_percentages_aggregates.GetMedian()},
       {"warm_function_percentage_percentile_0.01", warm_function_percentages_aggregates.GetPercentile(0.01)},
       {"warm_function_percentage_percentile_0.1", warm_function_percentages_aggregates.GetPercentile(0.1)},
       {"warm_function_percentage_percentile_1", warm_function_percentages_aggregates.GetPercentile(1)},
       {"warm_function_percentage_percentile_10", warm_function_percentages_aggregates.GetPercentile(10)},
       {"warm_function_percentage_std_dev", warm_function_percentages_aggregates.GetStandardDeviation()},
       {"warm_up_cost_usd", benchmark_result->GetOverallFunctionWarmUpCost()},
       {"function_cost_usd", static_cast<double>(CalculateOverallFunctionCost(
                                 benchmark_result, benchmark_parameters.function_instance_mb_size,
                                 benchmark_parameters.warm_up_strategy == "ProvisionedConcurrencyWarmUpStrategy"))}},
      {/*aggregated string metrics*/}, benchmark_result, {/*extract double metric functions*/},
      {[&](const InvocationResult& invocation_result) {
        return std::make_tuple("is_warm_function", is_warm_function(invocation_result) ? "true" : "false");
      }},
      {/*extract object metric functions*/});
}

}  // namespace skyrise
