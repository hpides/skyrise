#include "network_throughput_benchmark.hpp"

#include <algorithm>
#include <numeric>

#include <magic_enum.hpp>

#include "utils/string.hpp"
#include "utils/unit_conversion.hpp"

namespace skyrise {

const Aws::String kReadBucket = "network-throughput-benchmark-read";
const Aws::String kWriteBucket = "network-throughput-benchmark-write";

NetworkThroughputBenchmark::NetworkThroughputBenchmark(std::shared_ptr<BenchmarkHelper> helper,
                                                       std::shared_ptr<CostCalculator> cost_calculator,
                                                       const ExecuteMode execute_mode, const size_t num_iterations,
                                                       const std::vector<size_t>& function_instance_mb_sizes,
                                                       const std::vector<size_t>& object_byte_sizes,
                                                       const std::vector<size_t>& thread_counts)
    : NetworkBenchmark(std::move(helper), std::move(cost_calculator), execute_mode, num_iterations, kReadBucket,
                       kWriteBucket, function_instance_mb_sizes, object_byte_sizes, thread_counts) {}

Aws::Utils::Json::JsonValue NetworkThroughputBenchmark::GenerateResultOutput(
    const std::shared_ptr<std::vector<BenchmarkItemResult>>& result, const NetworkBenchmarkParameters& parameters) {
  Aws::StringStream benchmark_name;
  benchmark_name << "NetworkThroughputBenchmark/";
  benchmark_name << magic_enum::enum_name(execute_mode_) << "/" << parameters.function_instance_mb_size_
                 << "FunctionInstanceMB/" << ByteToMb(parameters.object_byte_size_) << "ObjectMB/"
                 << parameters.thread_count_ << "Threads/";
  benchmark_name << magic_enum::enum_name(parameters.operation_type_);

  const auto aggregates = BenchmarkHelper::CalculateAggregates(result, [&](const BenchmarkItemResult& single_result) {
    const double duration_seconds =
        std::chrono::duration<double>(
            std::chrono::duration<double, std::milli>(BenchmarkHelper::ExtractMetric(single_result, "duration_ms")))
            .count();

    return static_cast<double>(ByteToMb(parameters.object_byte_size_) * parameters.thread_count_ / duration_seconds);
  });

  return BenchmarkHelper::GenerateJsonOutput(
      benchmark_name.str(),
      {{"throughput_mb_per_s_average", aggregates.average},
       {"throughput_mb_per_s_minimum", aggregates.minimum},
       {"throughput_mb_per_s_median", aggregates.median},
       {"throughput_mb_per_s_maximum", aggregates.maximum},
       {"throughput_mb_per_s_percentile_90", aggregates.percentile_90},
       {"throughput_mb_per_s_percentile_99", aggregates.percentile_99},
       {"throughput_mb_per_s_percentile_99.9", aggregates.percentile_99_9},
       {"throughput_mb_per_s_percentile_99.99", aggregates.percentile_99_99},
       {"throughput_mb_per_s_std_dev", aggregates.standard_deviation},
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
