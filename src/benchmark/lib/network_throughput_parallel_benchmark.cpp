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

const size_t kBatchSize = 1;
const ExecuteMode kExecuteMode = ExecuteMode::kWarmParallel;
// TODO(d-justen): Change to the best performing parameters found by NetworkThroughputBenchmark
const size_t kFunctionInstanceMbSize = 3008;
const size_t kObjectByteSize = 16_MB;
const size_t kThreadCount = 4;

NetworkThroughputParallelBenchmark::NetworkThroughputParallelBenchmark(std::shared_ptr<BenchmarkHelper> helper,
                                                                       std::shared_ptr<CostCalculator> cost_calculator,
                                                                       const std::vector<size_t>& invocation_counts,
                                                                       const size_t repetition_count)
    : NetworkBenchmark(std::move(helper), std::move(cost_calculator), repetition_count, kBatchSize, {kObjectByteSize},
                       {kThreadCount}, invocation_counts) {
  for (const auto operation_type : {S3OperationType::kRead, S3OperationType::kWrite}) {
    Aws::StringStream function_name;
    function_name << "skyriseFunction" << (operation_type == S3OperationType::kRead ? "Read" : "Write") << "S3";

    for (const auto invocation_count : concurrent_invocation_counts_) {
      BenchmarkConfig config(function_name.str(), kFunctionInstanceMbSize, invocation_count, kExecuteMode,
                             repetition_count, std::vector<std::function<void()>>(repetition_count, [] {}));
      config.SetPayloads(GeneratePayloads(kFunctionInstanceMbSize, kObjectByteSize, kThreadCount,
                                          config.invocation_count_, operation_type));
      configs_.emplace_back(
          config, NetworkBenchmarkParameters{kFunctionInstanceMbSize, kObjectByteSize, kThreadCount, operation_type});
    }
  }
}

Aws::Utils::Json::JsonValue NetworkThroughputParallelBenchmark::GenerateResultOutput(
    const std::shared_ptr<BenchmarkResult>& result, const NetworkBenchmarkParameters& parameters) {
  Aws::StringStream benchmark_name;
  benchmark_name << "NetworkThroughputParallelBenchmark/" << parameters.function_instance_mb_size_
                 << "FunctionInstanceMB/" << ByteToMb(parameters.object_byte_size_) << "ObjectSizeMB/"
                 << parameters.thread_count_ << "Threads/" << magic_enum::enum_name(parameters.operation_type_);

  const auto invocation_results = result->GetInvocationResults();

  std::vector<double> throughputs;
  throughputs.reserve(invocation_results.size());

  for (size_t i = 0; i < invocation_results.size(); i++) {
    const double duration_seconds = result->GetRepetitionDuration(i).count();
    throughputs.emplace_back(ByteToMb(parameters.object_byte_size_) * parameters.thread_count_ *
                             invocation_results.front().size() / duration_seconds);
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
       {"benchmark_cost_usd",
        static_cast<double>(CalculateBenchmarkCost(result, parameters.function_instance_mb_size_))},
       {"benchmark_cost_overhead_usd", cost_overhead_ / configs_.size()}},
      {/*aggregated string metrics*/}, result,
      {[&](const InvocationResult& single_result) {
         Aws::Utils::Json::JsonValue result_value(StreamToString(&single_result.invoke_result_->GetPayload()));
         const auto duration_views = result_value.View().GetArray("ms_durations");
         const double duration_seconds =
             std::chrono::duration<double>(std::chrono::duration<double, std::milli>(duration_views[0].AsDouble()))
                 .count();
         return std::make_tuple("throughput_mb_per_s",
                                ByteToMb(parameters.object_byte_size_) / duration_seconds * parameters.thread_count_);
       },
       [&](const InvocationResult& single_result) {
         return std::make_tuple("billed_lambda_duration_ms",
                                BenchmarkHelper::ExtractBilledLambdaDuration(single_result));
       },
       [&](const InvocationResult& single_result) {
         return std::make_tuple("function_cost_usd", static_cast<double>(ExtractFunctionCost(
                                                         single_result, parameters.function_instance_mb_size_)));
       }},
      {/*extract string metric functions*/});
}

}  // namespace skyrise
