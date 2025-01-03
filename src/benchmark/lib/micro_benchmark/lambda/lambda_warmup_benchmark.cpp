#include "lambda_warmup_benchmark.hpp"

#include <thread>

#include "benchmark_result_aggregate.hpp"
#include "lambda_benchmark_output.hpp"
#include "micro_benchmark/micro_benchmark_utils.hpp"

namespace skyrise {

namespace {

const Aws::String kName = "lambdaWarmupBenchmark";
const Aws::String kFunctionName = "skyriseSimpleFunction";
const auto kExpectedInvocationResponseTemplate =
    Aws::Utils::Json::JsonValue().WithInteger("sleep_duration_seconds", 0).WithString("environment_id", "");

}  // namespace

LambdaWarmupBenchmark::LambdaWarmupBenchmark(std::shared_ptr<const CostCalculator> cost_calculator,
                                             const std::vector<size_t>& function_instance_sizes_mb,
                                             const std::vector<size_t>& concurrent_instance_counts,
                                             const size_t repetition_count, const bool enable_provisioned_concurrency,
                                             const std::vector<double>& provisioning_factors)
    : LambdaBenchmark(std::move(cost_calculator)) {
  configs_.reserve(function_instance_sizes_mb.size() * concurrent_instance_counts.size() *
                   (provisioning_factors.size() + 1));

  for (const auto function_instance_size_mb : function_instance_sizes_mb) {
    for (const auto concurrent_instance_count : concurrent_instance_counts) {
      for (const auto provisioning_factor : provisioning_factors) {
        const auto config = std::make_shared<LambdaBenchmarkConfig>(
            kFunctionName, "", Warmup::kYesOnEveryRepetition, DistinctFunctionPerRepetition::kYes, EventQueue::kNo,
            false, function_instance_size_mb, "", concurrent_instance_count, repetition_count);
        config->SetWarmupStrategy(std::make_shared<ConfigurableWarmupStrategy>(false, provisioning_factor));
        const LambdaWarmupBenchmarkParameters parameters{
            {function_instance_size_mb, concurrent_instance_count, repetition_count, 0},
            config->GetWarmupStrategy()->Name(),
            enable_provisioned_concurrency,
            provisioning_factor,
            0};
        configs_.emplace_back(parameters, config);
      }

      if (enable_provisioned_concurrency) {
        const auto config = std::make_shared<LambdaBenchmarkConfig>(
            kFunctionName, "", Warmup::kYesOnEveryRepetition, DistinctFunctionPerRepetition::kYes, EventQueue::kNo,
            false, function_instance_size_mb, "", concurrent_instance_count, repetition_count);
        config->SetWarmupStrategy(std::make_shared<ProvisionedConcurrencyWarmUpStrategy>());
        const LambdaWarmupBenchmarkParameters parameters{
            {function_instance_size_mb, concurrent_instance_count, repetition_count},
            config->GetWarmupStrategy()->Name(),
            enable_provisioned_concurrency,
            1.0};
        configs_.emplace_back(parameters, config);
      }
    }
  }
}

Aws::Utils::Array<Aws::Utils::Json::JsonValue> LambdaWarmupBenchmark::OnRun(
    const std::shared_ptr<LambdaBenchmarkRunner>& runner) {
  std::vector<std::shared_ptr<LambdaBenchmarkResult>> results;
  results.reserve(configs_.size());

  for (const auto& config : configs_) {
    results.push_back(runner->RunLambdaConfig(config.second));
    results.back()->ValidateInvocationResults(kExpectedInvocationResponseTemplate);
  }

  Aws::Utils::Array<Aws::Utils::Json::JsonValue> results_array(results.size());

  for (size_t i = 0; i < results.size(); ++i) {
    results_array[i] = GenerateResultOutput(results[i], configs_[i].first);
  }

  return results_array;
}

bool LambdaWarmupBenchmark::IsWarmFunction(const LambdaInvocationResult& invocation_result,
                                           const std::string& warmup_strategy) {
  while (!invocation_result.HasResultLog()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  return !invocation_result.GetResultLog()->HasInitDuration() ||
         warmup_strategy == "ProvisionedConcurrencyWarmUpStrategy";
}

Aws::Utils::Json::JsonValue LambdaWarmupBenchmark::GenerateResultOutput(
    const std::shared_ptr<LambdaBenchmarkResult>& result, const LambdaWarmupBenchmarkParameters& parameters) const {
  const auto& repetition_results = result->GetRepetitionResults();

  std::vector<double> warm_function_percentages;
  warm_function_percentages.reserve(parameters.base_parameters.repetition_count);

  for (const auto& repetition_result : repetition_results) {
    size_t successful_function_count = 0;
    size_t warm_function_count = 0;

    for (const auto& invocation_result : repetition_result.GetInvocationResults()) {
      if (invocation_result.IsSuccess()) {
        ++successful_function_count;
        if (IsWarmFunction(invocation_result, parameters.warmup_strategy)) {
          ++warm_function_count;
        }
      }
    }

    warm_function_percentages.emplace_back(warm_function_count / static_cast<double>(successful_function_count));
  }

  const BenchmarkResultAggregate aggregate(warm_function_percentages);

  return LambdaBenchmarkOutput(Name(), parameters.base_parameters, result)
      .WithInt64Argument("function_instance_size_mb", parameters.base_parameters.function_instance_size_mb)
      .WithInt64Argument("concurrent_instance_count", parameters.base_parameters.concurrent_instance_count)
      .WithInt64Argument("repetition_count", parameters.base_parameters.repetition_count)
      .WithStringArgument("warmup_strategy", parameters.warmup_strategy)
      .WithBoolArgument("enable_provisioned_concurrency", parameters.is_provisioned)
      .WithDoubleArgument("provisioning_factor", parameters.provisioning_factor)
      .WithDoubleMetric("benchmark_cost_usd",
                        static_cast<double>(result->CalculateRuntimeCost(
                            parameters.base_parameters.function_instance_size_mb,
                            parameters.warmup_strategy == "ProvisionedConcurrencyWarmUpStrategy")))
      .WithDoubleMetric("warmup_cost_usd", result->CalculateWarmupCost())
      .WithDoubleMetric("warm_function_percentage_minimum", aggregate.GetMinimum())
      .WithDoubleMetric("warm_function_percentage_maximum", aggregate.GetMaximum())
      .WithDoubleMetric("warm_function_percentage_average", aggregate.GetAverage())
      .WithDoubleMetric("warm_function_percentage_median", aggregate.GetMedian())
      .WithDoubleMetric("warm_function_percentage_percentile_0.01", aggregate.GetPercentile(0.01))
      .WithDoubleMetric("warm_function_percentage_percentile_0.1", aggregate.GetPercentile(0.1))
      .WithDoubleMetric("warm_function_percentage_percentile_1", aggregate.GetPercentile(1))
      .WithDoubleMetric("warm_function_percentage_percentile_10", aggregate.GetPercentile(10))
      .WithDoubleMetric("warm_function_percentage_std_dev", aggregate.GetStandardDeviation())
      .WithBoolInvocationMetric([&](const LambdaInvocationResult& invocation_result) {
        return std::make_tuple("is_warm_function", IsWarmFunction(invocation_result, parameters.warmup_strategy));
      })
      .Build();
}

const Aws::String& LambdaWarmupBenchmark::Name() const { return kName; }

}  // namespace skyrise
