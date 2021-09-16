#include "network_latency_benchmark.hpp"

#include <algorithm>
#include <numeric>

#include <magic_enum.hpp>

#include "benchmark_result_aggregate.hpp"
#include "utils/costs/pricing.hpp"
#include "utils/literal.hpp"
#include "utils/string.hpp"

namespace skyrise {

NetworkLatencyBenchmark::NetworkLatencyBenchmark(std::shared_ptr<const BenchmarkHelper> helper,
                                                 std::shared_ptr<const CostCalculator> cost_calculator,
                                                 const std::vector<size_t>& function_instance_mb_sizes,
                                                 const std::vector<size_t>& object_byte_sizes,
                                                 const std::vector<size_t>& batch_sizes, const size_t repetition_count)
    : NetworkBenchmark(std::move(helper), std::move(cost_calculator)) {
  for (const auto function_instance_mb_size : function_instance_mb_sizes) {
    for (const auto object_byte_size : object_byte_sizes) {
      for (const auto batch_size : batch_sizes) {
        for (const auto operation_type : {S3OperationType::kWrite, S3OperationType::kRead}) {
          Aws::StringStream function_name;
          function_name << "skyriseFunction" << (operation_type == S3OperationType::kRead ? "Read" : "Write") << "S3";

          const auto config =
              std::make_shared<LambdaBenchmarkConfig>(function_name.str(), function_instance_mb_size, repetition_count);
          NetworkBenchmarkParameters parameters{
              function_instance_mb_size, object_byte_size, batch_size, 1, 1, 1, operation_type};
          config->SetPayloads(GeneratePayloads(parameters));

          benchmark_configs_.emplace_back(parameters, config);
        }
      }
    }
  }
}

Aws::Utils::Json::JsonValue NetworkLatencyBenchmark::GenerateResultOutput(
    const std::shared_ptr<LambdaBenchmarkResult>& benchmark_result,
    const NetworkBenchmarkParameters& benchmark_parameters) {
  Aws::StringStream benchmark_name;
  benchmark_name << "NetworkLatencyBenchmark/" << benchmark_parameters.function_instance_mb_size
                 << "FunctionInstanceMB/" << std::string(magic_enum::enum_name(benchmark_parameters.operation_type))
                 << "/" << benchmark_parameters.object_byte_size << "ObjectByteSize";

  const auto benchmark_repetitions = benchmark_result->GetBenchmarkRepetitions();

  std::vector<double> latencies;
  latencies.reserve(benchmark_repetitions.size() * benchmark_repetitions.front().GetInvokeResults().size());

  for (const auto& benchmark_repetition : benchmark_repetitions) {
    for (const auto& invoke_result : benchmark_repetition.GetInvokeResults()) {
      if (invoke_result.IsSuccess()) {
        const auto ms_latencies = invoke_result.GetResponseBody().GetArray("ms_durations");

        for (size_t i = 0; i < ms_latencies.GetLength(); i++) {
          latencies.emplace_back(ms_latencies[i].AsDouble());
        }
      }
    }
  }

  if (latencies.empty()) {
    latencies.emplace_back(-1.0);
  }

  const BenchmarkResultAggregate aggregates(latencies);

  return GenerateJsonOutput(
      benchmark_name.str(),
      {{"latency_ms_minimum", aggregates.GetMinimum()},
       {"latency_ms_maximum", aggregates.GetMaximum()},
       {"latency_ms_average", aggregates.GetAverage()},
       {"latency_ms_median", aggregates.GetMedian()},
       {"latency_ms_percentile_90", aggregates.GetPercentile(90)},
       {"latency_ms_percentile_99", aggregates.GetPercentile(99)},
       {"latency_ms_percentile_99.9", aggregates.GetPercentile(99.9)},
       {"latency_ms_percentile_99.99", aggregates.GetPercentile(99.99)},
       {"latency_ms_std_dev", aggregates.GetStandardDeviation()},
       {"benchmark_cost_usd", static_cast<double>(CalculateOverallFunctionCost(
                                  benchmark_result, benchmark_parameters.function_instance_mb_size))}},
      {/*aggregated string metrics*/}, benchmark_result,
      {[&](const LambdaInvokeResult& invoke_result) {
         return std::make_tuple("billed_lambda_duration_ms", invoke_result.GetLogResult()->GetBilledDurationMs());
       },
       [&](const LambdaInvokeResult& invoke_result) {
         return std::make_tuple(
             "function_cost_usd",
             static_cast<double>(ExtractFunctionCost(invoke_result, benchmark_parameters.function_instance_mb_size)));
       }},
      {/*extract string metric functions*/}, {[&](const LambdaInvokeResult& invoke_result) {
        const auto ms_durations = invoke_result.GetResponseBody().GetArray("ms_durations");

        Aws::Utils::Array<Aws::Utils::Json::JsonValue> duration_seconds(ms_durations.GetLength());

        for (size_t i = 0; i < ms_durations.GetLength(); i++) {
          duration_seconds[i] = Aws::Utils::Json::JsonValue().AsDouble(
              std::chrono::duration<double>(std::chrono::duration<double, std::milli>(ms_durations[i].AsDouble()))
                  .count());
        }

        return std::make_tuple("duration_seconds", Aws::Utils::Json::JsonValue().AsArray(duration_seconds));
      }});
}

}  // namespace skyrise
