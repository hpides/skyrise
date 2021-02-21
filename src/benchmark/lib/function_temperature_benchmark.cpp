#include "function_temperature_benchmark.hpp"

#include <algorithm>
#include <chrono>

#include "benchmark_helper.hpp"
#include "benchmark_result_aggregate.hpp"
#include "client/client.hpp"

namespace skyrise {

FunctionTemperatureBenchmark::FunctionTemperatureBenchmark(std::shared_ptr<CostCalculator> cost_calculator,
                                                           const std::vector<size_t>& function_instance_mb_sizes,
                                                           const std::vector<size_t>& invocation_counts,
                                                           const size_t repetition_count)
    : Benchmark(std::move(cost_calculator)) {
  const auto payload_value = Aws::Utils::Json::JsonValue().WithBool("warmup", true).WithInteger("sleep_ms", kSleepMs);

  // TODO(anyone): Maybe move this into a constructor argument
  std::vector<std::shared_ptr<WarmUpStrategy>> warm_up_strategies{
      std::make_shared<SimpleWarmUpStrategy>(false), std::make_shared<SleepWarmUpStrategy>(false),
      std::make_shared<ProvisionedConcurrencyWarmUpStrategy>()};

  for (const auto function_instance_mb_size : function_instance_mb_sizes) {
    for (const auto invocation_count : invocation_counts) {
      for (const auto& warm_up_strategy : warm_up_strategies) {
        BenchmarkConfig config(kFunctionName, function_instance_mb_size, repetition_count, invocation_count,
                               WarmUp::kDefaultOncePerRepetition, UseOneFunctionPerRepetition::kYes);
        config.warm_up_strategy_ = warm_up_strategy;

        // We use the sleep mechanic after the warm up to ensure that the InvokeRequests are served by individual
        // Function instances
        config.SetOnePayloadForAllFunctions(std::make_shared<Aws::StringStream>(payload_value.View().WriteCompact()));

        benchmark_configs_.emplace_back(
            FunctionTemperatureBenchmarkParameters{function_instance_mb_size, invocation_count, repetition_count,
                                                   warm_up_strategy},
            config);
      }
    }
  }
}

Aws::Utils::Array<Aws::Utils::Json::JsonValue> FunctionTemperatureBenchmark::Run(
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

Aws::Utils::Json::JsonValue FunctionTemperatureBenchmark::GenerateResultOutput(
    const std::shared_ptr<BenchmarkResult>& benchmark_result,
    const FunctionTemperatureBenchmarkParameters& benchmark_parameters) const {
  Aws::StringStream benchmark_name;
  benchmark_name << "FunctionTemperatureBenchmark/" << benchmark_parameters.function_instance_mb_size << "/"
                 << benchmark_parameters.invocation_count << "/" << benchmark_parameters.warm_up_strategy->GetName();

  const auto is_warm_function = [&](const InvocationResult& invocation_result) {
    return !BenchmarkHelper::ExtractLogResultMetric(invocation_result, "Init Duration") ||
           benchmark_parameters.warm_up_strategy->GetName() == "ProvisionedConcurrencyWarmUpStrategy";
  };

  const auto& invocation_results = benchmark_result->GetInvocationResults();

  std::vector<double> warm_function_percentages;
  warm_function_percentages.reserve(benchmark_parameters.repetition_count);

  for (const auto& repetition_results : invocation_results) {
    size_t warm_function_count = 0;

    for (const auto& invocation_result : repetition_results) {
      if (is_warm_function(invocation_result.second)) {
        ++warm_function_count;
      }
    }

    warm_function_percentages.emplace_back(warm_function_count /
                                           static_cast<double>(benchmark_parameters.invocation_count));
  }

  const BenchmarkResultAggregate warm_function_percentages_aggregates(warm_function_percentages);

  std::vector<double> function_warm_up_cost(benchmark_result->GetFunctionWarmUpCosts().cbegin(),
                                            benchmark_result->GetFunctionWarmUpCosts().cend());

  const BenchmarkResultAggregate warm_up_cost_aggregates(function_warm_up_cost);

  return BenchmarkHelper::GenerateJsonOutput(
      benchmark_name.str(),
      {{"warm_function_percentages_minimum", warm_function_percentages_aggregates.GetMinimum()},
       {"warm_function_percentages_maximum", warm_function_percentages_aggregates.GetMaximum()},
       {"warm_function_percentages_average", warm_function_percentages_aggregates.GetAverage()},
       {"warm_function_percentages_median", warm_function_percentages_aggregates.GetMedian()},
       {"warm_function_percentages_percentile_0.01", warm_function_percentages_aggregates.GetPercentile(0.01)},
       {"warm_function_percentages_percentile_0.1", warm_function_percentages_aggregates.GetPercentile(0.1)},
       {"warm_function_percentages_percentile_1", warm_function_percentages_aggregates.GetPercentile(1)},
       {"warm_function_percentages_percentile_10", warm_function_percentages_aggregates.GetPercentile(10)},
       {"warm_function_percentages_std_dev", warm_function_percentages_aggregates.GetStandardDeviation()},
       {"warm_up_cost_usd", benchmark_result->GetOverallFunctionWarmUpCost()},
       {"function_cost_usd",
        static_cast<double>(CalculateOverallFunctionCost(
            benchmark_result, benchmark_parameters.function_instance_mb_size,
            benchmark_parameters.warm_up_strategy->GetName() == "ProvisionedConcurrencyWarmUpStrategy"))}},
      {/*aggregated string metrics*/}, benchmark_result, {/*extract double metric functions*/},
      {[&](const InvocationResult& invocation_result) {
        return std::make_tuple("is_warm_function", is_warm_function(invocation_result) ? "true" : "false");
      }},
      {/*extract object metric functions*/});
}

}  // namespace skyrise
