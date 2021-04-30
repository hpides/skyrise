#include "network_throughput_parallel_benchmark.hpp"

#include <algorithm>
#include <numeric>
#include <regex>
#include <tuple>
#include <unordered_map>

#include <magic_enum.hpp>

#include "benchmark_result_aggregate.hpp"
#include "utils/literal.hpp"
#include "utils/string.hpp"
#include "utils/unit_conversion.hpp"

namespace skyrise {

// TODO(d-justen): Change to the best performing parameters found by NetworkThroughputBenchmark
inline constexpr size_t kFunctionInstanceMbSize = 3008;
inline constexpr size_t kObjectByteSize = 16_MB;
inline constexpr size_t kThreadCount = 4;

NetworkThroughputParallelBenchmark::NetworkThroughputParallelBenchmark(std::shared_ptr<BenchmarkHelper> helper,
                                                                       std::shared_ptr<CostCalculator> cost_calculator,
                                                                       const std::vector<size_t>& invocation_counts,
                                                                       const size_t batch_size,
                                                                       const size_t repetition_count)
    : NetworkBenchmark(std::move(helper), std::move(cost_calculator), {kObjectByteSize}, {kThreadCount},
                       invocation_counts, batch_size, repetition_count) {
  for (const auto operation_type : {S3OperationType::kRead, S3OperationType::kWrite}) {
    Aws::StringStream function_name;
    function_name << "skyriseFunction" << (operation_type == S3OperationType::kRead ? "Read" : "Write") << "S3";

    for (const auto invocation_count : invocation_counts_) {
      BenchmarkConfig config(function_name.str(), kFunctionInstanceMbSize, repetition_count, invocation_count,
                             WarmUp::kDefault);
      config.SetPayloads(GeneratePayloads(kFunctionInstanceMbSize, kObjectByteSize, kThreadCount,
                                          config.concurrent_invocation_count_, operation_type));
      benchmark_configs_.emplace_back(
          NetworkBenchmarkParameters{kFunctionInstanceMbSize, kObjectByteSize, kThreadCount, operation_type}, config);
    }
  }
}

Aws::Utils::Json::JsonValue NetworkThroughputParallelBenchmark::GenerateResultOutput(
    const std::shared_ptr<BenchmarkResult>& benchmark_result, const NetworkBenchmarkParameters& benchmark_parameters) {
  Aws::StringStream benchmark_name;
  benchmark_name << "NetworkThroughputParallelBenchmark/" << benchmark_parameters.function_instance_mb_size
                 << "FunctionInstanceMB/" << ByteToMb(benchmark_parameters.object_byte_size) << "ObjectSizeMB/"
                 << benchmark_parameters.thread_count << "Threads/"
                 << magic_enum::enum_name(benchmark_parameters.operation_type);

  const auto& benchmark_repetitions = benchmark_result->GetBenchmarkRepetitions();

  std::vector<double> throughputs;
  throughputs.reserve(benchmark_repetitions.size());

  for (const auto& benchmark_repetition : benchmark_repetitions) {
    const double duration_seconds =
        std::chrono::duration<double>(std::chrono::duration<double, std::milli>(benchmark_repetition.GetDurationMs()))
            .count();
    throughputs.emplace_back(ByteToMb(benchmark_parameters.object_byte_size) * benchmark_parameters.thread_count *
                             batch_size_ * benchmark_repetition.GetInvokeResults().size() / duration_seconds);
  }

  const BenchmarkResultAggregate aggregates(throughputs);

  return BenchmarkHelper::GenerateJsonOutput(
      benchmark_name.str(),
      {{"throughput_parallel_mb_per_s_minimum", aggregates.GetMinimum()},
       {"throughput_parallel_mb_per_s_maximum", aggregates.GetMaximum()},
       {"throughput_parallel_mb_per_s_average", aggregates.GetAverage()},
       {"throughput_parallel_mb_per_s_median", aggregates.GetMedian()},
       {"throughput_parallel_mb_per_s_percentile_0.01", aggregates.GetPercentile(0.01)},
       {"throughput_parallel_mb_per_s_percentile_0.1", aggregates.GetPercentile(0.1)},
       {"throughput_parallel_mb_per_s_percentile_1", aggregates.GetPercentile(1)},
       {"throughput_parallel_mb_per_s_percentile_10", aggregates.GetPercentile(10)},
       {"throughput_parallel_mb_per_s_std_dev", aggregates.GetStandardDeviation()},
       {"benchmark_cost_usd", static_cast<double>(CalculateOverallFunctionCost(
                                  benchmark_result, benchmark_parameters.function_instance_mb_size))},
       {"benchmark_cost_overhead_usd", static_cast<double>(cost_overhead_ / benchmark_configs_.size())},
       {"warm_up_cost_usd", static_cast<double>(benchmark_result->GetWarmUpCost())}},
      {/*aggregated string metrics*/}, benchmark_result,
      {[&](const InvokeResult& invoke_result) {
         const auto duration_views = invoke_result.GetResponseBody().GetArray("ms_durations");
         const double duration_seconds =
             std::chrono::duration<double>(std::chrono::duration<double, std::milli>(duration_views[0].AsDouble()))
                 .count();
         return std::make_tuple("throughput_mb_per_s", ByteToMb(benchmark_parameters.object_byte_size) /
                                                           duration_seconds * benchmark_parameters.thread_count);
       },
       [&](const InvokeResult& invoke_result) {
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
