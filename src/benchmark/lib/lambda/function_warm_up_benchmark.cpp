#include "function_warm_up_benchmark.hpp"

#include <algorithm>
#include <chrono>
#include <functional>
#include <thread>

#include "benchmark_helper.hpp"
#include "benchmark_result_aggregate.hpp"
#include "client/client.hpp"
#include "lambda_benchmark_output.hpp"

namespace {

const Aws::String kName = "function_warm_up_benchmark";

}  // namespace

namespace skyrise {

FunctionWarmUpBenchmark::FunctionWarmUpBenchmark(std::shared_ptr<const CostCalculator> cost_calculator,
                                                 const std::vector<size_t>& function_instance_mb_sizes,
                                                 const std::vector<size_t>& invocation_counts,
                                                 const std::vector<size_t>& sleep_ms_durations,
                                                 const std::vector<double>& provisioning_factors,
                                                 const bool enable_provisioned_concurrency,
                                                 const size_t repetition_count)
    : LambdaBenchmark(std::move(cost_calculator)) {
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

          const auto config = std::make_shared<LambdaBenchmarkConfig>(
              kFunctionName, function_instance_mb_size, repetition_count, invocation_count,
              WarmUp::kDefaultOncePerRepetition, UseOneFunctionPerRepetition::kYes, UseEventQueue::kNo,
              after_repetition_callbacks);
          config->warm_up_strategy_ =
              std::make_shared<ConfigurableWarmUpStrategy>(false, sleep_ms_duration, provisioning_factor);

          config->SetOnePayloadForAllFunctions(
              std::make_shared<Aws::StringStream>(payload_value.View().WriteCompact()));

          benchmark_configs_.emplace_back(
              FunctionWarmUpBenchmarkParameters{function_instance_mb_size, invocation_count, sleep_ms_duration,
                                                provisioning_factor, enable_provisioned_concurrency, repetition_count,
                                                config->warm_up_strategy_->GetName()},
              config);
        }
      }

      if (enable_provisioned_concurrency) {
        const auto config = std::make_shared<LambdaBenchmarkConfig>(
            kFunctionName, function_instance_mb_size, repetition_count, invocation_count,
            WarmUp::kDefaultOncePerRepetition, UseOneFunctionPerRepetition::kYes);
        config->warm_up_strategy_ = std::make_shared<ProvisionedConcurrencyWarmUpStrategy>();

        config->SetOnePayloadForAllFunctions(std::make_shared<Aws::StringStream>(payload_value.View().WriteCompact()));

        benchmark_configs_.emplace_back(
            FunctionWarmUpBenchmarkParameters{function_instance_mb_size, invocation_count, 0, 1.0,
                                              enable_provisioned_concurrency, repetition_count,
                                              config->warm_up_strategy_->GetName()},
            config);
      }
    }
  }
}

Aws::Utils::Array<Aws::Utils::Json::JsonValue> FunctionWarmUpBenchmark::OnRun(
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

Aws::Utils::Json::JsonValue FunctionWarmUpBenchmark::GenerateResultOutput(
    const std::shared_ptr<LambdaBenchmarkResult>& benchmark_result,
    const FunctionWarmUpBenchmarkParameters& benchmark_parameters) const {
  const auto is_warm_function = [&](const LambdaInvokeResult& invoke_result) {
    return !invoke_result.GetLogResult()->HasInitDuration() ||
           benchmark_parameters.warm_up_strategy == "ProvisionedConcurrencyWarmUpStrategy";
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

  const BenchmarkResultAggregate aggregate(warm_function_percentages);

  return LambdaBenchmarkOutput(Name(), benchmark_result)
      .WithInt64Argument("function_instance_mb_size", benchmark_parameters.function_instance_mb_size)
      .WithInt64Argument("invocation_count", benchmark_parameters.invocation_count)
      .WithInt64Argument("sleep_ms_duration", benchmark_parameters.sleep_ms_duration)
      .WithDoubleArgument("provisioning_factor", benchmark_parameters.provisioning_factor)
      .WithBoolArgument("enable_provisioned_concurrency", benchmark_parameters.enable_provisioned_concurrency)
      .WithInt64Argument("repetition_count", benchmark_parameters.repetition_count)
      .WithStringArgument("warm_up_strategy", benchmark_parameters.warm_up_strategy)
      .WithDoubleMetric("benchmark_cost_usd",
                        static_cast<double>(CalculateOverallFunctionCost(
                            benchmark_result, benchmark_parameters.function_instance_mb_size,
                            benchmark_parameters.warm_up_strategy == "ProvisionedConcurrencyWarmUpStrategy")))
      .WithDoubleMetric("warm_up_cost_usd", benchmark_result->GetWarmUpCost())
      .WithDoubleMetric("warm_function_percentage_minimum", aggregate.GetMinimum())
      .WithDoubleMetric("warm_function_percentage_maximum", aggregate.GetMaximum())
      .WithDoubleMetric("warm_function_percentage_average", aggregate.GetAverage())
      .WithDoubleMetric("warm_function_percentage_median", aggregate.GetMedian())
      .WithDoubleMetric("warm_function_percentage_percentile_0.01", aggregate.GetPercentile(0.01))
      .WithDoubleMetric("warm_function_percentage_percentile_0.1", aggregate.GetPercentile(0.1))
      .WithDoubleMetric("warm_function_percentage_percentile_1", aggregate.GetPercentile(1))
      .WithDoubleMetric("warm_function_percentage_percentile_10", aggregate.GetPercentile(10))
      .WithDoubleMetric("warm_function_percentage_std_dev", aggregate.GetStandardDeviation())
      .WithBoolInvocationMetric([&](const LambdaInvokeResult& invoke_result) {
        return std::make_tuple("is_warm_function", is_warm_function(invoke_result));
      })
      .Build();
}

const Aws::String& FunctionWarmUpBenchmark::Name() const { return kName; }

}  // namespace skyrise
