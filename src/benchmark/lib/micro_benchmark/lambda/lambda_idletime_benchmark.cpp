#include "lambda_idletime_benchmark.hpp"

#include <chrono>
#include <set>
#include <thread>

#include "benchmark_result_aggregate.hpp"
#include "lambda_benchmark_output.hpp"
#include "utils/map.hpp"
#include "utils/string.hpp"

namespace skyrise {

namespace {

const Aws::String kName = "lambdaIdletimeBenchmark";
const Aws::String kFunctionName = "skyriseSimpleFunction";
const auto kExpectedInvocationResponseTemplate =
    Aws::Utils::Json::JsonValue().WithString("environment_id", "").WithInteger("sleep_duration_seconds", 0);

}  // namespace

LambdaIdletimeBenchmark::LambdaIdletimeBenchmark(std::shared_ptr<const CostCalculator> cost_calculator,
                                                 const std::vector<size_t>& function_instance_sizes_mb,
                                                 const std::vector<size_t>& concurrent_instance_counts,
                                                 const size_t repetition_count,
                                                 const std::vector<size_t>& after_repetition_delays_min)
    : LambdaBenchmark(std::move(cost_calculator)) {
  configs_.reserve(function_instance_sizes_mb.size() * concurrent_instance_counts.size() *
                   after_repetition_delays_min.size());

  const auto payload =
      std::make_shared<Aws::StringStream>(Aws::Utils::Json::JsonValue().WithBool("sleep", true).View().WriteCompact());

  for (const auto after_repetition_delay_min : after_repetition_delays_min) {
    std::vector<std::function<void()>> after_repetition_callbacks;
    after_repetition_callbacks.reserve(repetition_count + 1);

    for (size_t i = 0; i < repetition_count; ++i) {
      after_repetition_callbacks.emplace_back([after_repetition_delay_min]() {
        std::this_thread::sleep_for(std::chrono::minutes(after_repetition_delay_min));
      });
    }

    after_repetition_callbacks.emplace_back([]() {});

    for (const auto function_instance_size_mb : function_instance_sizes_mb) {
      for (const auto concurrent_instance_count : concurrent_instance_counts) {
        const auto config = std::make_shared<LambdaBenchmarkConfig>(
            kFunctionName, "", Warmup::kNo, DistinctFunctionPerRepetition::kNo, EventQueue::kNo, false,
            function_instance_size_mb, "", concurrent_instance_count, after_repetition_callbacks.size(),
            after_repetition_callbacks);

        config->SetOnePayloadForAllFunctions(payload);

        configs_.emplace_back(LambdaBenchmarkParameters{function_instance_size_mb, concurrent_instance_count,
                                                        repetition_count, after_repetition_delay_min},
                              config);
      }
    }
  }
}

Aws::Utils::Array<Aws::Utils::Json::JsonValue> LambdaIdletimeBenchmark::OnRun(
    const std::shared_ptr<LambdaBenchmarkRunner>& runner) {
  std::vector<std::shared_ptr<LambdaBenchmarkResult>> results;
  results.reserve(configs_.size());

  for (const auto& config : configs_) {
    results.emplace_back(runner->RunLambdaConfig(config.second));
    results.back()->ValidateInvocationResults(kExpectedInvocationResponseTemplate);
  }

  Aws::Utils::Array<Aws::Utils::Json::JsonValue> results_array(results.size());

  for (size_t i = 0; i < results.size(); ++i) {
    results_array[i] = GenerateResultOutput(results[i], configs_[i].first);
  }

  return results_array;
}

Aws::Utils::Json::JsonValue LambdaIdletimeBenchmark::GenerateResultOutput(
    const std::shared_ptr<LambdaBenchmarkResult>& result, const LambdaBenchmarkParameters& parameters) const {
  const auto& repetition_results = result->GetRepetitionResults();

  std::vector<double> idle_lifetime_percentages;
  idle_lifetime_percentages.reserve(repetition_results.size() - 1);

  std::set<std::string> initial_environment_ids;

  for (size_t i = 0; i < repetition_results.size(); ++i) {
    std::set<std::string> observed_environment_ids;

    for (const auto& invoke_result : repetition_results[i].GetInvocationResults()) {
      const Aws::String environment_id = invoke_result.GetEnvironmentId();

      if (i == 0) {
        initial_environment_ids.emplace(environment_id);
      } else if (initial_environment_ids.contains(environment_id)) {
        observed_environment_ids.emplace(environment_id);
      }
    }

    if (i > 0) {
      idle_lifetime_percentages.emplace_back(observed_environment_ids.size() /
                                             static_cast<double>(initial_environment_ids.size()));
    }
  }

  const BenchmarkResultAggregate aggregate(idle_lifetime_percentages);

  return LambdaBenchmarkOutput(Name(), parameters, result)
      .WithInt64Argument("function_instance_size_mb", parameters.function_instance_size_mb)
      .WithInt64Argument("concurrent_instance_count", parameters.concurrent_instance_count)
      .WithInt64Argument("repetition_count", parameters.repetition_count)
      .WithInt64Argument("after_repetition_delay_min", parameters.after_repetition_delay_min)
      .WithDoubleMetric("idle_lifetime_percentage_minimum", aggregate.GetMinimum())
      .WithDoubleMetric("idle_lifetime_percentage_maximum", aggregate.GetMaximum())
      .WithDoubleMetric("idle_lifetime_percentage_average", aggregate.GetAverage())
      .WithDoubleMetric("idle_lifetime_percentage_median", aggregate.GetMedian())
      .WithDoubleMetric("idle_lifetime_percentage_percentile_0.01", aggregate.GetPercentile(0.01))
      .WithDoubleMetric("idle_lifetime_percentage_percentile_0.1", aggregate.GetPercentile(0.1))
      .WithDoubleMetric("idle_lifetime_percentage_percentile_1", aggregate.GetPercentile(1))
      .WithDoubleMetric("idle_lifetime_percentage_percentile_10", aggregate.GetPercentile(10))
      .WithDoubleMetric("idle_lifetime_percentage_std_dev", aggregate.GetStandardDeviation())
      .WithDoubleInvocationMetric([&](const LambdaInvocationResult& invocation_result) {
        return std::make_tuple("invocation_cost_usd",
                               invocation_result.CalculateCost(cost_calculator_, parameters.function_instance_size_mb));
      })
      .WithStringInvocationMetric([&](const LambdaInvocationResult& invocation_result) {
        return std::make_tuple("environment_id", invocation_result.GetResponseBody().AsString());
      })
      .Build();
}

const Aws::String& LambdaIdletimeBenchmark::Name() const { return kName; }

}  // namespace skyrise
