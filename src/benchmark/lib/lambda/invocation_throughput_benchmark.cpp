#include "invocation_throughput_benchmark.hpp"

#include <algorithm>
#include <array>

#include <magic_enum.hpp>

#include "benchmark_result_aggregate.hpp"
#include "lambda_benchmark_output.hpp"
#include "utils/string.hpp"

namespace skyrise {

InvocationThroughputBenchmark::InvocationThroughputBenchmark(std::shared_ptr<const CostCalculator> cost_calculator,
                                                             const std::vector<size_t>& function_instance_mb_sizes,
                                                             const std::vector<size_t>& invocation_counts,
                                                             const std::vector<size_t>& function_payload_byte_sizes,
                                                             const size_t repetition_count)
    : LambdaBenchmark(std::move(cost_calculator)) {
  std::array<UseEventQueue, 2> use_event_queues{UseEventQueue::kYes, UseEventQueue::kNo};

  benchmark_configs_.reserve(function_instance_mb_sizes.size() * invocation_counts.size() *
                             function_payload_byte_sizes.size() * use_event_queues.size());

  for (const auto function_instance_mb_size : function_instance_mb_sizes) {
    for (const auto invocation_count : invocation_counts) {
      for (const auto function_payload_byte_size : function_payload_byte_sizes) {
        for (const auto use_event_queue : use_event_queues) {
          const auto config = std::make_shared<LambdaBenchmarkConfig>(
              kFunctionName, function_instance_mb_size, repetition_count, invocation_count, WarmUp::kDefault,
              UseOneFunctionPerRepetition::kNo, use_event_queue);

          if (function_payload_byte_size > 0) {
            config->SetOnePayloadForAllFunctions(BenchmarkHelper::GenerateRandomObject(function_payload_byte_size));
          }

          benchmark_configs_.emplace_back(
              InvocationThroughputBenchmarkParameters{function_instance_mb_size, invocation_count,
                                                      function_payload_byte_size, repetition_count, use_event_queue},
              config);
        }
      }
    }
  }
}

Aws::Utils::Array<Aws::Utils::Json::JsonValue> InvocationThroughputBenchmark::OnRun(
    const std::shared_ptr<LambdaBenchmarkRunner>& benchmark_runner) {
  std::vector<std::shared_ptr<LambdaBenchmarkResult>> benchmark_results;
  benchmark_results.reserve(benchmark_configs_.size());

  for (const auto& benchmark_config : benchmark_configs_) {
    benchmark_results.emplace_back(benchmark_runner->RunLambdaConfig(benchmark_config.second));
  }

  Aws::Utils::Array<Aws::Utils::Json::JsonValue> benchmark_outputs(benchmark_results.size());

  for (size_t i = 0; i < benchmark_results.size(); ++i) {
    benchmark_outputs[i] =
        InvocationThroughputBenchmark::GenerateResultOutput(benchmark_results[i], benchmark_configs_[i].first);
  }

  return benchmark_outputs;
}

Aws::Utils::Json::JsonValue InvocationThroughputBenchmark::GenerateResultOutput(
    const std::shared_ptr<LambdaBenchmarkResult>& benchmark_result,
    const InvocationThroughputBenchmarkParameters& benchmark_parameters) const {
  const auto& benchmark_repetitions = benchmark_result->GetBenchmarkRepetitions();

  std::vector<double> invocation_throughputs;
  invocation_throughputs.reserve(benchmark_repetitions.size());

  for (const auto& benchmark_repetition : benchmark_repetitions) {
    auto min_start_time = std::chrono::system_clock::time_point::max();
    auto max_end_time = std::chrono::system_clock::time_point::min();

    for (const auto& invoke_result : benchmark_repetition.GetInvokeResults()) {
      min_start_time = std::min(min_start_time, invoke_result.GetStartPoint());
      max_end_time = std::max(max_end_time, invoke_result.GetEndPoint());
    }

    const double duration = std::chrono::duration<double>(max_end_time - min_start_time).count();
    const double throughput = benchmark_parameters.invocation_count / duration;

    invocation_throughputs.emplace_back(throughput);
  }

  const BenchmarkResultAggregate aggregate(invocation_throughputs);

  return LambdaBenchmarkOutput("invocation_throughput_benchmark", benchmark_result)
      .WithInt64Argument("function_instance_mb_size", benchmark_parameters.function_instance_mb_size)
      .WithInt64Argument("invocation_count", benchmark_parameters.invocation_count)
      .WithInt64Argument("function_payload_byte_size", benchmark_parameters.function_payload_byte_size)
      .WithInt64Argument("repetition_count", benchmark_parameters.repetition_count)
      .WithStringArgument("use_event_queue", std::string(magic_enum::enum_name(benchmark_parameters.use_event_queue)))
      .WithDoubleMetric("invocation_throughput_functions_per_s_minimum", aggregate.GetMinimum())
      .WithDoubleMetric("invocation_throughput_functions_per_s_maximum", aggregate.GetMaximum())
      .WithDoubleMetric("invocation_throughput_functions_per_s_average", aggregate.GetAverage())
      .WithDoubleMetric("invocation_throughput_functions_per_s_median", aggregate.GetMedian())
      .WithDoubleMetric("invocation_throughput_functions_per_s_percentile_0.01", aggregate.GetPercentile(0.01))
      .WithDoubleMetric("invocation_throughput_functions_per_s_percentile_0.1", aggregate.GetPercentile(0.1))
      .WithDoubleMetric("invocation_throughput_functions_per_s_percentile_1", aggregate.GetPercentile(1))
      .WithDoubleMetric("invocation_throughput_functions_per_s_percentile_10", aggregate.GetPercentile(10))
      .WithDoubleMetric("invocation_throughput_functions_per_s_std_dev", aggregate.GetStandardDeviation())
      .WithDoubleMetric("benchmark_cost_usd", static_cast<double>(CalculateOverallFunctionCost(
                                                  benchmark_result, benchmark_parameters.function_instance_mb_size)))
      .WithDoubleMetric("warm_up_cost_usd", static_cast<double>(benchmark_result->GetWarmUpCost()))
      .WithDoubleInvocationMetric([&](const LambdaInvokeResult& invoke_result) {
        return std::make_tuple("function_cost_usd",
                               ExtractFunctionCost(invoke_result, benchmark_parameters.function_instance_mb_size));
      })
      .WithDoubleInvocationMetric([&](const LambdaInvokeResult& invoke_result) {
        return std::make_tuple(
            "duration",
            std::chrono::duration<double>(invoke_result.GetEndPoint() - invoke_result.GetStartPoint()).count());
      })
      .Build();
}

}  // namespace skyrise
