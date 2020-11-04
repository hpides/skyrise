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
  benchmark_name << magic_enum::enum_name(execute_mode_) << "/" << parameters.function_instance_mb_size_ << "MB/"
                 << ByteToMb(parameters.object_byte_size_) << "MB/" << parameters.thread_count_;

  const auto extract_duration_seconds = [&](const BenchmarkItemResult& single_result, const Aws::String& key) {
    return std::chrono::duration<double>(
               std::chrono::duration<double, std::milli>(BenchmarkHelper::ExtractMetric(single_result, key)))
        .count();
  };

  const auto get_throughput_aggregates =
      BenchmarkHelper::CalculateAggregates(result, [&](const BenchmarkItemResult& single_result) {
        return static_cast<double>(ByteToMb(parameters.object_byte_size_) * parameters.thread_count_ /
                                   extract_duration_seconds(single_result, kJsonGetObjectDurationKey));
      });

  const auto put_throughput_aggregates =
      BenchmarkHelper::CalculateAggregates(result, [&](const BenchmarkItemResult& single_result) {
        return static_cast<double>(ByteToMb(parameters.object_byte_size_) * parameters.thread_count_ /
                                   extract_duration_seconds(single_result, kJsonPutObjectDurationKey));
      });

  return BenchmarkHelper::GenerateJsonOutput(
      benchmark_name.str(),
      {{"get_object_throughput_mb_per_s_average", get_throughput_aggregates.average},
       {"get_object_throughput_mb_per_s_minimum", get_throughput_aggregates.minimum},
       {"get_object_throughput_mb_per_s_median", get_throughput_aggregates.median},
       {"get_object_throughput_mb_per_s_maximum", get_throughput_aggregates.maximum},
       {"get_object_throughput_mb_per_s_percentile_90", get_throughput_aggregates.percentile_90},
       {"get_object_throughput_mb_per_s_percentile_99", get_throughput_aggregates.percentile_99},
       {"get_object_throughput_mb_per_s_percentile_99.9", get_throughput_aggregates.percentile_99_9},
       {"get_object_throughput_mb_per_s_percentile_99.99", get_throughput_aggregates.percentile_99_99},
       {"get_object_throughput_mb_per_s_std_dev", get_throughput_aggregates.standard_deviation},
       {"put_object_throughput_mb_per_s_average", put_throughput_aggregates.average},
       {"put_object_throughput_mb_per_s_minimum", put_throughput_aggregates.minimum},
       {"put_object_throughput_mb_per_s_median", put_throughput_aggregates.median},
       {"put_object_throughput_mb_per_s_maximum", put_throughput_aggregates.maximum},
       {"put_object_throughput_mb_per_s_percentile_90", put_throughput_aggregates.percentile_90},
       {"put_object_throughput_mb_per_s_percentile_99", put_throughput_aggregates.percentile_99},
       {"put_object_throughput_mb_per_s_percentile_99.9", put_throughput_aggregates.percentile_99_9},
       {"put_object_throughput_mb_per_s_percentile_99.99", put_throughput_aggregates.percentile_99_99},
       {"put_object_throughput_mb_per_s_std_dev", put_throughput_aggregates.standard_deviation},
       {"benchmark_cost_usd",
        static_cast<double>(CalculateBenchmarkCost(result, parameters.function_instance_mb_size_))},
       {"benchmark_cost_overhead_usd", cost_overhead_ / configs_.size()}},
      {/*aggregated string metrics*/}, result,
      {[&](const BenchmarkItemResult& single_result) {
         const double duration_seconds =
             std::chrono::duration<double>(std::chrono::duration<double, std::milli>(BenchmarkHelper::ExtractMetric(
                                               single_result, kJsonGetObjectDurationKey)))
                 .count();
         return std::make_tuple("get_object_throughput_mb_per_s",
                                ByteToMb(parameters.object_byte_size_) / duration_seconds * parameters.thread_count_);
       },
       [&](const BenchmarkItemResult& single_result) {
         const double duration_seconds =
             std::chrono::duration<double>(std::chrono::duration<double, std::milli>(BenchmarkHelper::ExtractMetric(
                                               single_result, kJsonPutObjectDurationKey)))
                 .count();
         return std::make_tuple("put_object_throughput_mb_per_s",
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
