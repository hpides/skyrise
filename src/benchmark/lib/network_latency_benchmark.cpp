#include "network_latency_benchmark.hpp"

#include <algorithm>
#include <numeric>

#include <magic_enum.hpp>

#include "benchmark_result_aggregate.hpp"
#include "utils/costs/pricing.hpp"
#include "utils/literal.hpp"
#include "utils/string.hpp"

namespace skyrise {

NetworkLatencyBenchmark::NetworkLatencyBenchmark(std::shared_ptr<BenchmarkHelper> helper,
                                                 std::shared_ptr<CostCalculator> cost_calculator,
                                                 const std::vector<size_t>& function_instance_mb_sizes,
                                                 const std::vector<size_t>& object_byte_sizes_read,
                                                 const std::vector<size_t>& object_byte_sizes_write,
                                                 const size_t batch_size, const size_t repetition_count)
    : NetworkBenchmark(std::move(helper), std::move(cost_calculator), object_byte_sizes_read, {1}, {1}, batch_size,
                       repetition_count) {
  for (const auto function_instance_mb_size : function_instance_mb_sizes) {
    for (const size_t object_byte_size_read : object_byte_sizes_read) {
      BenchmarkConfig config("skyriseFunctionReadS3", function_instance_mb_size, repetition_count_);
      config.SetPayloads(GeneratePayloads(function_instance_mb_size, object_byte_size_read, 1,
                                          config.concurrent_invocation_count_, S3OperationType::kRead));
      benchmark_configs_.emplace_back(
          NetworkBenchmarkParameters{function_instance_mb_size, object_byte_size_read, 1, S3OperationType::kRead},
          config);
    }

    for (const size_t object_byte_size_write : object_byte_sizes_write) {
      BenchmarkConfig config("skyriseFunctionWriteS3", function_instance_mb_size, repetition_count_);
      config.SetPayloads(GeneratePayloads(function_instance_mb_size, object_byte_size_write, 1,
                                          config.concurrent_invocation_count_, S3OperationType::kWrite));
      benchmark_configs_.emplace_back(
          NetworkBenchmarkParameters{function_instance_mb_size, object_byte_size_write, 1, S3OperationType::kWrite},
          config);
    }
  }
}

Aws::Utils::Json::JsonValue NetworkLatencyBenchmark::GenerateResultOutput(
    const std::shared_ptr<BenchmarkResult>& benchmark_result, const NetworkBenchmarkParameters& benchmark_parameters) {
  Aws::StringStream benchmark_name;
  benchmark_name << "NetworkLatencyBenchmark/" << benchmark_parameters.function_instance_mb_size
                 << "FunctionInstanceMB/" << std::string(magic_enum::enum_name(benchmark_parameters.operation_type))
                 << "/" << benchmark_parameters.object_byte_size << "ObjectByteSize";

  const auto benchmark_repetitions = benchmark_result->GetBenchmarkRepetitions();

  std::vector<double> latencies;
  latencies.reserve(benchmark_repetitions.size() * benchmark_repetitions.front().GetInvokeResults().size());

  for (const auto& benchmark_repetition : benchmark_repetitions) {
    for (const auto& invoke_result : benchmark_repetition.GetInvokeResults()) {
      const auto ms_latencies = invoke_result.GetResponseBody().GetArray("ms_durations");

      for (size_t i = 0; i < ms_latencies.GetLength(); i++) {
        latencies.emplace_back(ms_latencies[i].AsDouble());
      }
    }
  }

  const BenchmarkResultAggregate aggregates(latencies);

  return BenchmarkHelper::GenerateJsonOutput(
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
                                  benchmark_result, benchmark_parameters.function_instance_mb_size))},
       {"benchmark_cost_overhead_usd", static_cast<double>(cost_overhead_ / benchmark_configs_.size())}},
      {/*aggregated string metrics*/}, benchmark_result,
      {[&](const InvokeResult& invoke_result) {
         return std::make_tuple("billed_lambda_duration_ms", invoke_result.GetLogResult()->GetBilledDurationMs());
       },
       [&](const InvokeResult& invoke_result) {
         return std::make_tuple(
             "function_cost_usd",
             static_cast<double>(ExtractFunctionCost(invoke_result, benchmark_parameters.function_instance_mb_size)));
       }},
      {/*extract string metric functions*/}, {[&](const InvokeResult& invoke_result) {
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
