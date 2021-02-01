#include "network_throughput_benchmark.hpp"

#include <algorithm>
#include <numeric>
#include <tuple>

#include <magic_enum.hpp>

#include "benchmark_result_aggregate.hpp"
#include "utils/string.hpp"
#include "utils/unit_conversion.hpp"

namespace skyrise {

NetworkThroughputBenchmark::NetworkThroughputBenchmark(std::shared_ptr<BenchmarkHelper> helper,
                                                       std::shared_ptr<CostCalculator> cost_calculator,
                                                       const std::vector<size_t>& function_instance_mb_sizes,
                                                       const std::vector<size_t>& object_byte_sizes,
                                                       const std::vector<size_t>& thread_counts,
                                                       const size_t repetition_count, const size_t batch_size)
    : NetworkBenchmark(std::move(helper), std::move(cost_calculator), repetition_count, batch_size, object_byte_sizes,
                       thread_counts, {1}) {
  for (const auto function_instance_mb_size : function_instance_mb_sizes) {
    for (const auto object_byte_size : object_byte_sizes) {
      for (const auto thread_count : thread_counts) {
        if (thread_count * object_byte_size <= MbToByte(function_instance_mb_size) / 2) {
          for (const auto operation_type : {S3OperationType::kRead, S3OperationType::kWrite}) {
            Aws::StringStream function_name;
            function_name << "skyriseFunction" << (operation_type == S3OperationType::kRead ? "Read" : "Write") << "S3";

            BenchmarkConfig config(function_name.str(), function_instance_mb_size, repetition_count_ / batch_size_, 1,
                                   WarmUpStrategy::kNone);
            config.SetPayloads(GeneratePayloads(function_instance_mb_size, object_byte_size, thread_count,
                                                config.concurrent_invocation_count_, operation_type));
            configs_.emplace_back(config, NetworkBenchmarkParameters{function_instance_mb_size, object_byte_size,
                                                                     thread_count, operation_type});
          }
        }
      }
    }
  }
}

Aws::Utils::Json::JsonValue NetworkThroughputBenchmark::GenerateResultOutput(
    const std::shared_ptr<BenchmarkResult>& result, const NetworkBenchmarkParameters& parameters) {
  Aws::StringStream benchmark_name;
  benchmark_name << "NetworkThroughputBenchmark/" << parameters.function_instance_mb_size_ << "FunctionInstanceMB/"
                 << ByteToMb(parameters.object_byte_size_) << "ObjectMB/" << parameters.thread_count_ << "Threads/"
                 << magic_enum::enum_name(parameters.operation_type_);

  const auto invocation_results = result->GetInvocationResults().front();

  const auto batched_runs = GenerateBatchedSubResultOutput(
      invocation_results, benchmark_name.str(), parameters.function_instance_mb_size_, "duration_seconds",
      [&](const double value) {
        return std::chrono::duration<double>(std::chrono::duration<double, std::milli>(value)).count();
      });

  const auto seconds_durations = ExtractValuesFromBatchedSubResults(batched_runs, "duration_seconds");

  std::vector<double> throughputs;
  throughputs.reserve(seconds_durations.size());

  std::transform(seconds_durations.cbegin(), seconds_durations.cend(), std::back_inserter(throughputs),
                 [&](const double seconds_duration) {
                   return ByteToMb(parameters.object_byte_size_) * parameters.thread_count_ / seconds_duration;
                 });

  const BenchmarkResultAggregate aggregates(throughputs);

  return BenchmarkHelper::GenerateJsonOutput(
      benchmark_name.str(),
      {{"throughput_mb_per_s_minimum", aggregates.GetMinimum()},
       {"throughput_mb_per_s_maximum", aggregates.GetMaximum()},
       {"throughput_mb_per_s_average", aggregates.GetAverage()},
       {"throughput_mb_per_s_median", aggregates.GetMedian()},
       {"throughput_mb_per_s_percentile_0.01", aggregates.GetPercentile(0.01)},
       {"throughput_mb_per_s_percentile_0.1", aggregates.GetPercentile(0.1)},
       {"throughput_mb_per_s_percentile_1", aggregates.GetPercentile(1)},
       {"throughput_mb_per_s_percentile_10", aggregates.GetPercentile(10)},
       {"throughput_mb_per_s_std_dev", aggregates.GetStandardDeviation()},
       {"benchmark_cost_usd",
        static_cast<double>(CalculateBenchmarkCost(result, parameters.function_instance_mb_size_))},
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
