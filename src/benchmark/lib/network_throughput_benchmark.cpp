#include "network_throughput_benchmark.hpp"

#include <algorithm>
#include <numeric>
#include <tuple>

#include <magic_enum.hpp>

#include "utils/string.hpp"
#include "utils/unit_conversion.hpp"

namespace skyrise {

NetworkThroughputBenchmark::NetworkThroughputBenchmark(std::shared_ptr<BenchmarkHelper> helper,
                                                       std::shared_ptr<CostCalculator> cost_calculator,
                                                       const std::vector<size_t>& function_instance_mb_sizes,
                                                       const std::vector<size_t>& object_byte_sizes,
                                                       const std::vector<size_t>& thread_counts,
                                                       const size_t num_iterations)
    : NetworkBenchmark(std::move(helper), std::move(cost_calculator), ExecuteMode::kWarmSequential, num_iterations) {
  for (const auto function_instance_mb_size : function_instance_mb_sizes) {
    for (const auto object_byte_size : object_byte_sizes) {
      for (const auto thread_count : thread_counts) {
        if (thread_count * object_byte_size <= MbToByte(function_instance_mb_size) / 2) {
          for (const auto operation_type : {S3OperationType::kRead, S3OperationType::kWrite}) {
            Aws::StringStream function_name;
            function_name << "skyriseFunction" << (operation_type == S3OperationType::kRead ? "Read" : "Write") << "S3";

            BenchmarkConfig config(function_name.str(), function_instance_mb_size, num_iterations_, execute_mode_);
            config.SetPayloads(GeneratePayloads(function_instance_mb_size, object_byte_size, thread_count,
                                                operation_type, num_iterations_));
            configs_.emplace_back(config, NetworkBenchmarkParameters{function_instance_mb_size, object_byte_size,
                                                                     thread_count, operation_type});
          }
        }
      }
    }
  }
}

Aws::Utils::Json::JsonValue NetworkThroughputBenchmark::GenerateResultOutput(
    const std::shared_ptr<std::vector<BenchmarkItemResult>>& result, const NetworkBenchmarkParameters& parameters) {
  Aws::StringStream benchmark_name;
  benchmark_name << "NetworkThroughputBenchmark/" << parameters.function_instance_mb_size_ << "FunctionInstanceMB/"
                 << ByteToMb(parameters.object_byte_size_) << "ObjectMB/" << parameters.thread_count_ << "Threads/"
                 << magic_enum::enum_name(parameters.operation_type_);

  const auto aggregates = BenchmarkHelper::CalculateAggregates(result, [&](const BenchmarkItemResult& single_result) {
    return std::chrono::duration<double>(
               std::chrono::duration<double, std::milli>(BenchmarkHelper::ExtractMetric(single_result, "duration_ms")))
        .count();
  });

  const auto to_throughput = [&](const double duration_seconds) {
    return static_cast<double>(ByteToMb(parameters.object_byte_size_) * parameters.thread_count_ / duration_seconds);
  };

  return BenchmarkHelper::GenerateJsonOutput(
      benchmark_name.str(),
      {{"throughput_mb_per_s_average", to_throughput(aggregates.average)},
       {"throughput_mb_per_s_minimum", to_throughput(aggregates.maximum)},
       {"throughput_mb_per_s_median", to_throughput(aggregates.median)},
       {"throughput_mb_per_s_maximum", to_throughput(aggregates.minimum)},
       {"throughput_mb_per_s_percentile_10", to_throughput(aggregates.percentile_90)},
       {"throughput_mb_per_s_percentile_1", to_throughput(aggregates.percentile_99)},
       {"throughput_mb_per_s_percentile_0.1", to_throughput(aggregates.percentile_99_9)},
       {"throughput_mb_per_s_percentile_0.01", to_throughput(aggregates.percentile_99_99)},
       {"benchmark_cost_usd",
        static_cast<double>(CalculateBenchmarkCost(result, parameters.function_instance_mb_size_))},
       {"benchmark_cost_overhead_usd", cost_overhead_ / configs_.size()}},
      {/*aggregated string metrics*/}, result,
      {[&](const BenchmarkItemResult& single_result) {
         const double duration_seconds =
             std::chrono::duration<double>(std::chrono::duration<double, std::milli>(
                                               BenchmarkHelper::ExtractMetric(single_result, "duration_ms")))
                 .count();
         return std::make_tuple("throughput_mb_per_s",
                                ByteToMb(parameters.object_byte_size_) / duration_seconds * parameters.thread_count_);
       },
       [&](const BenchmarkItemResult& single_result) {
         return std::make_tuple("billed_lambda_duration_ms",
                                BenchmarkHelper::ExtractBilledLambdaDuration(single_result));
       },
       [&](const BenchmarkItemResult& single_result) {
         return std::make_tuple("function_cost_usd", static_cast<double>(ExtractFunctionCost(
                                                         single_result, parameters.function_instance_mb_size_)));
       }},
      {/*extract string metric functions*/});
}

}  // namespace skyrise
