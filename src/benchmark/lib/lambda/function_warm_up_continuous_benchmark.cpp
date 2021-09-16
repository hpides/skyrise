#include "function_warm_up_continuous_benchmark.hpp"

#include <algorithm>
#include <chrono>
#include <thread>

#include "benchmark_helper.hpp"
#include "benchmark_result_aggregate.hpp"
#include "client/client.hpp"
#include "utils/map.hpp"

namespace skyrise {

FunctionWarmUpContinuousBenchmark::FunctionWarmUpContinuousBenchmark(
    std::shared_ptr<const CostCalculator> cost_calculator, const std::vector<size_t>& function_instance_mb_sizes,
    const std::vector<size_t>& invocation_counts, const std::vector<size_t>& sleep_ms_durations,
    const std::vector<double>& provisioning_factors, const std::vector<size_t>& warm_up_min_intervals,
    const size_t repetition_count)
    : LambdaBenchmark(std::move(cost_calculator)) {
  benchmark_configs_.reserve(function_instance_mb_sizes.size() * invocation_counts.size() * sleep_ms_durations.size() *
                             provisioning_factors.size() * warm_up_min_intervals.size());

  for (const auto function_instance_mb_size : function_instance_mb_sizes) {
    for (const auto invocation_count : invocation_counts) {
      for (const auto sleep_ms_duration : sleep_ms_durations) {
        for (const auto provisioning_factor : provisioning_factors) {
          for (const auto warm_up_min_interval : warm_up_min_intervals) {
            std::vector<std::function<void()>> after_repetition_callbacks(repetition_count - 1, [&]() {
              std::this_thread::sleep_for(std::chrono::minutes(warm_up_min_interval));
            });
            after_repetition_callbacks.emplace_back([&]() {});

            const auto config = std::make_shared<LambdaBenchmarkConfig>(
                kFunctionName, function_instance_mb_size, repetition_count,
                static_cast<size_t>(invocation_count * provisioning_factor), WarmUp::kNone,
                UseOneFunctionPerRepetition::kNo, UseEventQueue::kNo, after_repetition_callbacks);

            const auto payload_value =
                Aws::Utils::Json::JsonValue().WithBool("warmup", true).WithInteger("sleep_ms", sleep_ms_duration);
            config->SetOnePayloadForAllFunctions(
                std::make_shared<Aws::StringStream>(payload_value.View().WriteCompact()));

            benchmark_configs_.emplace_back(
                FunctionWarmUpContinuousBenchmarkParameters{function_instance_mb_size, invocation_count,
                                                            sleep_ms_duration, provisioning_factor,
                                                            warm_up_min_interval, repetition_count},
                config);
          }
        }
      }
    }
  }
}

Aws::Utils::Array<Aws::Utils::Json::JsonValue> FunctionWarmUpContinuousBenchmark::OnRun(
    const std::shared_ptr<LambdaBenchmarkRunner>& benchmark_runner) {
  std::vector<std::shared_ptr<LambdaBenchmarkResult>> benchmark_results;

  for (const auto& config : benchmark_configs_) {
    benchmark_results.emplace_back(benchmark_runner->RunLambdaConfig(config.second));
  }

  Aws::Utils::Array<Aws::Utils::Json::JsonValue> benchmark_outputs(benchmark_results.size());

  for (size_t i = 0; i < benchmark_results.size(); ++i) {
    benchmark_outputs[i] = GenerateResultOutput(benchmark_results[i], benchmark_configs_[i].first);
  }

  return benchmark_outputs;
}

Aws::Utils::Json::JsonValue FunctionWarmUpContinuousBenchmark::GenerateResultOutput(
    const std::shared_ptr<LambdaBenchmarkResult>& benchmark_result,
    const FunctionWarmUpContinuousBenchmarkParameters& benchmark_parameters) const {
  Aws::StringStream benchmark_name;
  benchmark_name << "FunctionWarmUpContinuousBenchmark/" << benchmark_parameters.function_instance_mb_size << "/"
                 << benchmark_parameters.invocation_count << "/" << benchmark_parameters.sleep_ms_duration << "/"
                 << benchmark_parameters.provisioning_factor << "/" << benchmark_parameters.warm_up_min_interval << "/"
                 << benchmark_parameters.repetition_count;

  const auto is_warm_function = [&](const LambdaInvokeResult& invoke_result) {
    return !invoke_result.GetLogResult()->HasInitDuration();
  };

  const auto& benchmark_repetitions = benchmark_result->GetBenchmarkRepetitions();

  std::vector<double> warm_function_percentages;
  warm_function_percentages.reserve(benchmark_parameters.repetition_count);

  for (const auto& benchmark_repetition : benchmark_repetitions) {
    size_t successful_function_count = 0;
    size_t warm_function_count = 0;

    for (const auto& invoke_result : benchmark_repetition.GetInvokeResults()) {
      if (invoke_result.IsSuccess()) {
        ++successful_function_count;

        if (is_warm_function(invoke_result)) {
          ++warm_function_count;
        }
      }
    }

    warm_function_percentages.emplace_back(warm_function_count / static_cast<double>(successful_function_count));
  }

  std::vector<std::tuple<Aws::String, double>> numeric_metrics;
  numeric_metrics.reserve(warm_function_percentages.size() + 11);

  for (size_t i = 0; i < warm_function_percentages.size(); ++i) {
    numeric_metrics.emplace_back("warm_function_percentage_" + std::to_string(i), warm_function_percentages[i]);
  }

  const BenchmarkResultAggregate warm_function_percentages_aggregates(warm_function_percentages);

  numeric_metrics.emplace_back("warm_function_percentage_minimum", warm_function_percentages_aggregates.GetMinimum());
  numeric_metrics.emplace_back("warm_function_percentage_maximum", warm_function_percentages_aggregates.GetMaximum());
  numeric_metrics.emplace_back("warm_function_percentage_average", warm_function_percentages_aggregates.GetAverage());
  numeric_metrics.emplace_back("warm_function_percentage_median", warm_function_percentages_aggregates.GetMedian());
  numeric_metrics.emplace_back("warm_function_percentage_percentile_0.01",
                               warm_function_percentages_aggregates.GetPercentile(0.01));
  numeric_metrics.emplace_back("warm_function_percentage_percentile_0.1",
                               warm_function_percentages_aggregates.GetPercentile(0.1));
  numeric_metrics.emplace_back("warm_function_percentage_percentile_1",
                               warm_function_percentages_aggregates.GetPercentile(1));
  numeric_metrics.emplace_back("warm_function_percentage_percentile_10",
                               warm_function_percentages_aggregates.GetPercentile(10));
  numeric_metrics.emplace_back("warm_function_percentage_std_dev",
                               warm_function_percentages_aggregates.GetStandardDeviation());
  numeric_metrics.emplace_back("warm_up_cost_usd", benchmark_result->GetWarmUpCost());
  numeric_metrics.emplace_back("benchmark_cost_usd",
                               static_cast<double>(CalculateOverallFunctionCost(
                                   benchmark_result, benchmark_parameters.function_instance_mb_size)));

  return GenerateJsonOutput(benchmark_name.str(), numeric_metrics, {/*aggregated string metrics*/}, benchmark_result,
                            {/*extract double metric functions*/}, {[&](const LambdaInvokeResult& invoke_result) {
                              return std::make_tuple("is_warm_function",
                                                     is_warm_function(invoke_result) ? "true" : "false");
                            }},
                            {/*extract object metric functions*/});
}

}  // namespace skyrise
