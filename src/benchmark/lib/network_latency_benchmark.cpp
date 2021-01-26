#include "network_latency_benchmark.hpp"

#include <algorithm>
#include <numeric>

#include <magic_enum.hpp>

#include "benchmark_result_aggregate.hpp"
#include "utils/costs/pricing.hpp"
#include "utils/literal.hpp"
#include "utils/string.hpp"

namespace skyrise {

const size_t kBatchSize = 100;

NetworkLatencyBenchmark::NetworkLatencyBenchmark(std::shared_ptr<BenchmarkHelper> helper,
                                                 std::shared_ptr<CostCalculator> cost_calculator,
                                                 const std::vector<size_t>& function_instance_mb_sizes,
                                                 const std::vector<size_t>& object_byte_sizes_read,
                                                 const std::vector<size_t>& object_byte_sizes_write,
                                                 const size_t repetition_count)
    : NetworkBenchmark(std::move(helper), std::move(cost_calculator), repetition_count, kBatchSize,
                       object_byte_sizes_read, {1}, {1}) {
  for (const auto function_instance_mb_size : function_instance_mb_sizes) {
    for (const size_t object_byte_size_read : object_byte_sizes_read) {
      BenchmarkConfig config("skyriseFunctionReadS3", function_instance_mb_size, repetition_count_ / batch_size_, 1,
                             WarmUpStrategy::kNone);
      config.SetPayloads(GeneratePayloads(function_instance_mb_size, object_byte_size_read, 1,
                                          config.concurrent_invocation_count_, S3OperationType::kRead));
      configs_.emplace_back(config, NetworkBenchmarkParameters{function_instance_mb_size, object_byte_size_read, 1,
                                                               S3OperationType::kRead});
    }

    for (const size_t object_byte_size_write : object_byte_sizes_write) {
      BenchmarkConfig config("skyriseFunctionWriteS3", function_instance_mb_size, repetition_count_ / batch_size_, 1,
                             WarmUpStrategy::kNone);
      config.SetPayloads(GeneratePayloads(function_instance_mb_size, object_byte_size_write, 1,
                                          config.concurrent_invocation_count_, S3OperationType::kWrite));
      configs_.emplace_back(config, NetworkBenchmarkParameters{function_instance_mb_size, object_byte_size_write, 1,
                                                               S3OperationType::kWrite});
    }
  }
}

Aws::Utils::Json::JsonValue NetworkLatencyBenchmark::GenerateResultOutput(
    const std::shared_ptr<BenchmarkResult>& result, const NetworkBenchmarkParameters& parameters) {
  Aws::StringStream benchmark_name;
  benchmark_name << "NetworkLatencyBenchmark/" << parameters.function_instance_mb_size_ << "FunctionInstanceMB/"
                 << std::string(magic_enum::enum_name(parameters.operation_type_)) << "/"
                 << parameters.object_byte_size_ << "ObjectByteSize";

  const auto invocation_results = result->GetInvocationResults().front();

  const auto batched_runs =
      GenerateBatchedSubResultOutput(invocation_results, benchmark_name.str(), parameters.function_instance_mb_size_,
                                     "ms_latencies", [](const double value) { return value; });

  const BenchmarkResultAggregate aggregates(ExtractValuesFromBatchedSubResults(batched_runs, "ms_latencies"));

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
       {"benchmark_cost_usd", CalculateBenchmarkCost(result, parameters.function_instance_mb_size_)},
       {"benchmark_cost_overhead_usd", cost_overhead_ / configs_.size()}},
      {/*aggregated string metrics*/}, result,
      {[&](const InvocationResult& single_result) {
         return std::make_tuple("billed_lambda_duration_ms",
                                BenchmarkHelper::ExtractBilledLambdaDuration(single_result));
       },
       [&](const InvocationResult& single_result) {
         return std::make_tuple("function_cost_usd", static_cast<double>(ExtractFunctionCost(
                                                         single_result, parameters.function_instance_mb_size_)));
       }},
      {/*extract string metric functions*/}, {[&](const InvocationResult& single_result) {
        const Aws::Utils::Json::JsonValue payload_value(StreamToString(&single_result.invoke_result_->GetPayload()));
        const auto ms_durations = payload_value.View().GetArray("ms_durations");

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
