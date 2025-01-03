#include "lambda_colocation_benchmark.hpp"

#include <numeric>
#include <thread>

#include "benchmark_result_aggregate.hpp"
#include "lambda_benchmark_output.hpp"
#include "micro_benchmark/micro_benchmark_utils.hpp"
#include "utils/map.hpp"

namespace skyrise {

namespace {

const Aws::String kName = "lambdaColocationBenchmark";
const Aws::String kFunctionName = "skyriseSimpleFunction";

}  // namespace

LambdaColocationBenchmark::LambdaColocationBenchmark(std::shared_ptr<const CostCalculator> cost_calculator,
                                                     const std::vector<size_t>& function_instance_sizes_mb,
                                                     const std::vector<size_t>& concurrent_instance_counts,
                                                     const size_t repetition_count)
    : LambdaBenchmark(std::move(cost_calculator)) {
  benchmark_configs_.reserve(function_instance_sizes_mb.size() * concurrent_instance_counts.size());

  const auto payload =
      std::make_shared<Aws::StringStream>(Aws::Utils::Json::JsonValue().WithBool("sleep", true).View().WriteCompact());

  for (const auto function_instance_size_mb : function_instance_sizes_mb) {
    for (const auto concurrent_instance_count : concurrent_instance_counts) {
      const auto config = std::make_shared<LambdaBenchmarkConfig>(
          kFunctionName, "", Warmup::kNo, DistinctFunctionPerRepetition::kNo, EventQueue::kNo, false,
          function_instance_size_mb, "", concurrent_instance_count, repetition_count);

      config->SetOnePayloadForAllFunctions(payload);

      benchmark_configs_.emplace_back(
          LambdaBenchmarkParameters{function_instance_size_mb, concurrent_instance_count, repetition_count}, config);
    }
  }
}

const Aws::String& LambdaColocationBenchmark::Name() const { return kName; }

Aws::Utils::Array<Aws::Utils::Json::JsonValue> LambdaColocationBenchmark::OnRun(
    const std::shared_ptr<LambdaBenchmarkRunner>& runner) {
  std::vector<std::shared_ptr<LambdaBenchmarkResult>> benchmark_results;
  benchmark_results.reserve(benchmark_configs_.size());

  for (const auto& benchmark_config : benchmark_configs_) {
    benchmark_results.emplace_back(runner->RunLambdaConfig(benchmark_config.second));
  }

  Aws::Utils::Array<Aws::Utils::Json::JsonValue> benchmark_outputs(benchmark_results.size());

  for (size_t i = 0; i < benchmark_results.size(); ++i) {
    benchmark_outputs[i] =
        LambdaColocationBenchmark::GenerateResultOutput(benchmark_results[i], benchmark_configs_[i].first);
  }

  return benchmark_outputs;
}

Aws::Utils::Json::JsonValue LambdaColocationBenchmark::GenerateResultOutput(
    const std::shared_ptr<LambdaBenchmarkResult>& result, const LambdaBenchmarkParameters& parameters) const {
  const auto& benchmark_repetitions = result->GetRepetitionResults();

  std::vector<double> colocation_counts;

  for (const auto& benchmark_repetition : benchmark_repetitions) {
    std::map<std::string, size_t> environment_ids_to_colocation_counts;

    for (const auto& invocation_result : benchmark_repetition.GetInvocationResults()) {
      const std::string environment_id = invocation_result.GetResponseBody().GetString("environment_id");

      if (!environment_ids_to_colocation_counts.contains(environment_id)) {
        environment_ids_to_colocation_counts.emplace(environment_id, 1);
      } else {
        ++environment_ids_to_colocation_counts[environment_id];
      }
    }

    for (const auto& [environment_id, colocation_count] : environment_ids_to_colocation_counts) {
      colocation_counts.emplace_back(static_cast<double>(colocation_count));
    }
  }

  const BenchmarkResultAggregate aggregate(colocation_counts);

  return LambdaBenchmarkOutput(Name(), parameters, result)
      .WithInt64Argument("function_instance_size_mb", parameters.function_instance_size_mb)
      .WithInt64Argument("concurrent_instance_count", parameters.concurrent_instance_count)
      .WithInt64Argument("repetition_count", parameters.repetition_count)
      .WithDoubleMetric("colocation_counts_minimum", aggregate.GetMinimum())
      .WithDoubleMetric("colocation_counts_maximum", aggregate.GetMaximum())
      .WithDoubleMetric("colocation_counts_average", aggregate.GetAverage())
      .WithDoubleMetric("colocation_counts_median", aggregate.GetMedian())
      .WithDoubleMetric("colocation_counts_percentile_90", aggregate.GetPercentile(90))
      .WithDoubleMetric("colocation_counts_percentile_99", aggregate.GetPercentile(99))
      .WithDoubleMetric("colocation_counts_percentile_99.9", aggregate.GetPercentile(99.9))
      .WithDoubleMetric("colocation_counts_percentile_99.99", aggregate.GetPercentile(99.99))
      .WithDoubleMetric("colocation_counts_std_dev", aggregate.GetStandardDeviation())
      .WithDoubleInvocationMetric([&](const LambdaInvocationResult& invocation_result) {
        return std::make_tuple("invocation_cost_usd",
                               invocation_result.CalculateCost(cost_calculator_, parameters.function_instance_size_mb));
      })
      .WithStringInvocationMetric([&](const LambdaInvocationResult& invocation_result) {
        return std::make_tuple("environment_id", invocation_result.GetResponseBody().GetString("environment_id"));
      })
      .Build();
}

}  // namespace skyrise
