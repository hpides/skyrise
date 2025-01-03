#include "lambda_startup_throughput_benchmark.hpp"

#include <array>

#include <magic_enum/magic_enum.hpp>

#include "benchmark_result_aggregate.hpp"
#include "lambda_benchmark_output.hpp"
#include "micro_benchmark/micro_benchmark_utils.hpp"

namespace {

const Aws::String kName = "lambdaStartupThroughputBenchmark";
const Aws::String kFunctionName = "skyriseSimpleFunction";

}  // namespace

namespace skyrise {

LambdaStartupThroughputBenchmark::LambdaStartupThroughputBenchmark(
    std::shared_ptr<const CostCalculator> cost_calculator, const std::vector<size_t>& function_instance_sizes_mb,
    const std::vector<size_t>& concurrent_instance_counts, const size_t repetition_count,
    const std::vector<size_t>& payload_byte_sizes)
    : LambdaBenchmark(std::move(cost_calculator)) {
  const std::array<EventQueue, 2> invocation_options{EventQueue::kYes, EventQueue::kNo};

  benchmark_configs_.reserve(function_instance_sizes_mb.size() * concurrent_instance_counts.size() *
                             payload_byte_sizes.size() * invocation_options.size());

  for (const auto function_instance_size_mb : function_instance_sizes_mb) {
    for (const auto concurrent_instance_count : concurrent_instance_counts) {
      for (const auto payload_byte_size : payload_byte_sizes) {
        for (const auto is_event : invocation_options) {
          const auto config = std::make_shared<LambdaBenchmarkConfig>(
              kFunctionName, "", Warmup::kYesOnEveryRepetition, DistinctFunctionPerRepetition::kNo, is_event, false,
              function_instance_size_mb, "", concurrent_instance_count, repetition_count);

          if (payload_byte_size > 0) {
            config->SetOnePayloadForAllFunctions(GenerateRandomObject(payload_byte_size));
          }

          benchmark_configs_.emplace_back(
              LambdaStartupBenchmarkParameters{
                  {function_instance_size_mb, concurrent_instance_count, repetition_count, 0},
                  "",
                  payload_byte_size,
                  is_event,
                  Warmup::kNo},
              config);
        }
      }
    }
  }
}

Aws::Utils::Array<Aws::Utils::Json::JsonValue> LambdaStartupThroughputBenchmark::OnRun(
    const std::shared_ptr<LambdaBenchmarkRunner>& runner) {
  std::vector<std::shared_ptr<LambdaBenchmarkResult>> benchmark_results;
  benchmark_results.reserve(benchmark_configs_.size());

  for (const auto& benchmark_config : benchmark_configs_) {
    benchmark_results.emplace_back(runner->RunLambdaConfig(benchmark_config.second));
  }

  Aws::Utils::Array<Aws::Utils::Json::JsonValue> benchmark_outputs(benchmark_results.size());

  for (size_t i = 0; i < benchmark_results.size(); ++i) {
    benchmark_outputs[i] =
        LambdaStartupThroughputBenchmark::GenerateResultOutput(benchmark_results[i], benchmark_configs_[i].first);
  }

  return benchmark_outputs;
}

Aws::Utils::Json::JsonValue LambdaStartupThroughputBenchmark::GenerateResultOutput(
    const std::shared_ptr<LambdaBenchmarkResult>& result, const LambdaStartupBenchmarkParameters& parameters) const {
  const auto& benchmark_repetitions = result->GetRepetitionResults();

  std::vector<double> invocation_throughputs;
  invocation_throughputs.reserve(benchmark_repetitions.size());

  for (const auto& benchmark_repetition : benchmark_repetitions) {
    auto min_start_time = std::chrono::system_clock::time_point::max();
    auto max_end_time = std::chrono::system_clock::time_point::min();

    for (const auto& invoke_result : benchmark_repetition.GetInvocationResults()) {
      min_start_time = std::min(min_start_time, invoke_result.GetStartPoint());
      max_end_time = std::max(max_end_time, invoke_result.GetEndPoint());
    }

    const double duration = std::chrono::duration<double>(max_end_time - min_start_time).count();
    const double throughput = parameters.base_parameters.concurrent_instance_count / duration;

    invocation_throughputs.emplace_back(throughput);
  }

  const BenchmarkResultAggregate aggregate(invocation_throughputs);

  return LambdaBenchmarkOutput(Name(), parameters.base_parameters, result)
      .WithInt64Argument("function_instance_size_mb", parameters.base_parameters.function_instance_size_mb)
      .WithInt64Argument("concurrent_instance_count", parameters.base_parameters.concurrent_instance_count)
      .WithInt64Argument("repetition_count", parameters.base_parameters.repetition_count)
      .WithInt64Argument("function_payload_byte_size", parameters.payload_size_bytes)
      .WithStringArgument("use_event_queue", std::string(magic_enum::enum_name(parameters.is_event)))
      .WithDoubleMetric("warmup_cost_usd", static_cast<double>(result->CalculateWarmupCost()))
      .WithDoubleMetric("invocation_throughput_functions_per_s_minimum", aggregate.GetMinimum())
      .WithDoubleMetric("invocation_throughput_functions_per_s_maximum", aggregate.GetMaximum())
      .WithDoubleMetric("invocation_throughput_functions_per_s_average", aggregate.GetAverage())
      .WithDoubleMetric("invocation_throughput_functions_per_s_median", aggregate.GetMedian())
      .WithDoubleMetric("invocation_throughput_functions_per_s_percentile_0.01", aggregate.GetPercentile(0.01))
      .WithDoubleMetric("invocation_throughput_functions_per_s_percentile_0.1", aggregate.GetPercentile(0.1))
      .WithDoubleMetric("invocation_throughput_functions_per_s_percentile_1", aggregate.GetPercentile(1))
      .WithDoubleMetric("invocation_throughput_functions_per_s_percentile_10", aggregate.GetPercentile(10))
      .WithDoubleMetric("invocation_throughput_functions_per_s_std_dev", aggregate.GetStandardDeviation())
      .WithDoubleInvocationMetric([&](const LambdaInvocationResult& invocation_result) {
        return std::make_tuple(
            "invocation_cost_usd",
            invocation_result.CalculateCost(cost_calculator_, parameters.base_parameters.function_instance_size_mb));
      })
      .WithDoubleInvocationMetric([&](const LambdaInvocationResult& invoke_result) {
        return std::make_tuple(
            "duration",
            std::chrono::duration<double>(invoke_result.GetEndPoint() - invoke_result.GetStartPoint()).count());
      })
      .Build();
}

const Aws::String& LambdaStartupThroughputBenchmark::Name() const { return kName; }

}  // namespace skyrise
