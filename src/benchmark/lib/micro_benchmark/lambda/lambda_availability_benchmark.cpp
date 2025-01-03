#include "lambda_availability_benchmark.hpp"

#include <chrono>
#include <numeric>
#include <thread>

#include "benchmark_result_aggregate.hpp"
#include "lambda_benchmark_output.hpp"
#include "utils/map.hpp"
#include "utils/string.hpp"

namespace skyrise {

namespace {

const Aws::String kName = "lambdaAvailabilityBenchmark";
const Aws::String kFunctionName = "skyriseSimpleFunction";
const auto kExpectedInvocationResponseTemplate =
    Aws::Utils::Json::JsonValue().WithString("environment_id", "").WithInteger("sleep_duration_seconds", 0);

}  // namespace

LambdaAvailabilityBenchmark::LambdaAvailabilityBenchmark(std::shared_ptr<const CostCalculator> cost_calculator,
                                                         const std::vector<size_t>& function_instance_sizes_mb,
                                                         const std::vector<size_t>& concurrent_instance_counts,
                                                         const size_t repetition_count,
                                                         const std::vector<size_t>& after_repetition_delays_min)
    : LambdaBenchmark(std::move(cost_calculator)) {
  benchmark_configs_.reserve(function_instance_sizes_mb.size() * concurrent_instance_counts.size() *
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

        benchmark_configs_.emplace_back(LambdaBenchmarkParameters{function_instance_size_mb, concurrent_instance_count,
                                                                  repetition_count, after_repetition_delay_min},
                                        config);
      }
    }
  }
}

Aws::Utils::Array<Aws::Utils::Json::JsonValue> LambdaAvailabilityBenchmark::OnRun(
    const std::shared_ptr<LambdaBenchmarkRunner>& runner) {
  std::vector<std::shared_ptr<LambdaBenchmarkResult>> results;
  results.reserve(benchmark_configs_.size());

  for (const auto& benchmark_config : benchmark_configs_) {
    results.emplace_back(runner->RunLambdaConfig(benchmark_config.second));
    results.back()->ValidateInvocationResults(kExpectedInvocationResponseTemplate);
  }

  Aws::Utils::Array<Aws::Utils::Json::JsonValue> benchmark_outputs(results.size());

  for (size_t i = 0; i < results.size(); ++i) {
    benchmark_outputs[i] = LambdaAvailabilityBenchmark::GenerateResultOutput(results[i], benchmark_configs_[i].first);
  }

  return benchmark_outputs;
}

Aws::Utils::Json::JsonValue LambdaAvailabilityBenchmark::GenerateResultOutput(
    const std::shared_ptr<LambdaBenchmarkResult>& result, const LambdaBenchmarkParameters& parameters) const {
  const auto& benchmark_repetitions = result->GetRepetitionResults();

  std::map<std::string, std::vector<bool>> environment_ids_to_availability_flags;

  for (size_t i = 0; i < benchmark_repetitions.size(); ++i) {
    for (const auto& invoke_result : benchmark_repetitions[i].GetInvocationResults()) {
      const std::string environment_id = invoke_result.GetEnvironmentId();

      if (i == 0) {
        environment_ids_to_availability_flags.try_emplace(environment_id, parameters.repetition_count, false);
      } else if (environment_ids_to_availability_flags.contains(environment_id)) {
        environment_ids_to_availability_flags[environment_id][i] = true;
      }
    }
  }

  std::vector<double> availability_percentages;
  availability_percentages.reserve(environment_ids_to_availability_flags.size());

  std::vector<double> unavailable_phases_counts;
  unavailable_phases_counts.reserve(environment_ids_to_availability_flags.size());

  std::vector<double> unavailable_phases_lengths;
  unavailable_phases_lengths.reserve(environment_ids_to_availability_flags.size());

  for (const auto& [environment_id, availability_flags] : environment_ids_to_availability_flags) {
    bool is_available = true;

    size_t available_repetition_count = 0;
    size_t unavailable_phases_count = 0;
    size_t unavailable_phases_length = 0;

    for (size_t i = 0; i < availability_flags.size(); ++i) {
      if (availability_flags[i]) {
        ++unavailable_phases_length;

        if (is_available) {
          ++unavailable_phases_count;
          is_available = false;
        }

        if (i == availability_flags.size() - 1) {
          unavailable_phases_lengths.emplace_back(static_cast<double>(unavailable_phases_length));
        }
      } else {
        ++available_repetition_count;

        if (!is_available) {
          unavailable_phases_lengths.emplace_back(static_cast<double>(unavailable_phases_length));
          unavailable_phases_length = 0;
          is_available = true;
        }
      }
    }

    availability_percentages.emplace_back(available_repetition_count /
                                          static_cast<double>(parameters.repetition_count));
    unavailable_phases_counts.emplace_back(static_cast<double>(unavailable_phases_count));
  }

  if (unavailable_phases_lengths.empty()) {
    unavailable_phases_lengths.emplace_back(0);
  }

  const BenchmarkResultAggregate availability_percentages_aggregate(availability_percentages);
  const BenchmarkResultAggregate unavailable_phases_counts_aggregate(unavailable_phases_counts);
  const BenchmarkResultAggregate unavailable_phases_lengths_aggregate(unavailable_phases_lengths);

  return LambdaBenchmarkOutput(Name(), parameters, result)
      .WithInt64Argument("function_instance_size_mb", parameters.function_instance_size_mb)
      .WithInt64Argument("concurrent_instance_count", parameters.concurrent_instance_count)
      .WithInt64Argument("repetition_count", parameters.repetition_count)
      .WithInt64Argument("after_repetition_delay_min", parameters.after_repetition_delay_min)
      .WithDoubleMetric("availability_percentage_minimum", availability_percentages_aggregate.GetMinimum())
      .WithDoubleMetric("availability_percentage_maximum", availability_percentages_aggregate.GetMaximum())
      .WithDoubleMetric("availability_percentage_average", availability_percentages_aggregate.GetAverage())
      .WithDoubleMetric("availability_percentage_median", availability_percentages_aggregate.GetMedian())
      .WithDoubleMetric("availability_percentage_percentile_0.01",
                        availability_percentages_aggregate.GetPercentile(0.01))
      .WithDoubleMetric("availability_percentage_percentile_0.1", availability_percentages_aggregate.GetPercentile(0.1))
      .WithDoubleMetric("availability_percentage_percentile_1", availability_percentages_aggregate.GetPercentile(1))
      .WithDoubleMetric("availability_percentage_percentile_10", availability_percentages_aggregate.GetPercentile(10))
      .WithDoubleMetric("availability_percentage_std_dev", availability_percentages_aggregate.GetStandardDeviation())
      .WithDoubleMetric("unavailable_phases_count_minimum", unavailable_phases_counts_aggregate.GetMinimum())
      .WithDoubleMetric("unavailable_phases_count_maximum", unavailable_phases_counts_aggregate.GetMaximum())
      .WithDoubleMetric("unavailable_phases_count_average", unavailable_phases_counts_aggregate.GetAverage())
      .WithDoubleMetric("unavailable_phases_count_median", unavailable_phases_counts_aggregate.GetMedian())
      .WithDoubleMetric("unavailable_phases_count_percentile_90", unavailable_phases_counts_aggregate.GetPercentile(90))
      .WithDoubleMetric("unavailable_phases_count_percentile_99", unavailable_phases_counts_aggregate.GetPercentile(99))
      .WithDoubleMetric("unavailable_phases_count_percentile_99.9",
                        unavailable_phases_counts_aggregate.GetPercentile(99.9))
      .WithDoubleMetric("unavailable_phases_count_percentile_99.99",
                        unavailable_phases_counts_aggregate.GetPercentile(99.99))
      .WithDoubleMetric("unavailable_phases_count_std_dev", unavailable_phases_counts_aggregate.GetStandardDeviation())
      .WithDoubleMetric("unavailable_phases_length_minimum", unavailable_phases_lengths_aggregate.GetMinimum())
      .WithDoubleMetric("unavailable_phases_length_maximum", unavailable_phases_lengths_aggregate.GetMaximum())
      .WithDoubleMetric("unavailable_phases_length_average", unavailable_phases_lengths_aggregate.GetAverage())
      .WithDoubleMetric("unavailable_phases_length_median", unavailable_phases_lengths_aggregate.GetMedian())
      .WithDoubleMetric("unavailable_phases_length_percentile_90",
                        unavailable_phases_lengths_aggregate.GetPercentile(90))
      .WithDoubleMetric("unavailable_phases_length_percentile_99",
                        unavailable_phases_lengths_aggregate.GetPercentile(99))
      .WithDoubleMetric("unavailable_phases_length_percentile_99.9",
                        unavailable_phases_lengths_aggregate.GetPercentile(99.9))
      .WithDoubleMetric("unavailable_phases_length_percentile_99.99",
                        unavailable_phases_lengths_aggregate.GetPercentile(99.99))
      .WithDoubleMetric("unavailable_phases_length_std_dev",
                        unavailable_phases_lengths_aggregate.GetStandardDeviation())
      .Build();
}

const Aws::String& LambdaAvailabilityBenchmark::Name() const { return kName; }

}  // namespace skyrise
