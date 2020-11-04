#include "network_latency_benchmark.hpp"

#include <algorithm>
#include <numeric>

#include <magic_enum.hpp>

#include "utils/costs/pricing.hpp"
#include "utils/literal.hpp"

namespace skyrise {

const size_t kObjectBytesSize = 1_KB;
const Aws::String kReadBucket = "network-latency-benchmark-read";
const Aws::String kWriteBucket = "network-latency-benchmark-write";

NetworkLatencyBenchmark::NetworkLatencyBenchmark(std::shared_ptr<BenchmarkHelper> helper,
                                                 std::shared_ptr<CostCalculator> cost_calculator,
                                                 const std::vector<size_t>& function_instance_mb_sizes,
                                                 const ExecuteMode execute_mode, const size_t num_iterations)
    : NetworkBenchmark(std::move(helper), std::move(cost_calculator), execute_mode, num_iterations, kReadBucket,
                       kWriteBucket, function_instance_mb_sizes, {kObjectBytesSize}, {1}) {}

Aws::Utils::Json::JsonValue NetworkLatencyBenchmark::GenerateResultOutput(
    const std::shared_ptr<std::vector<BenchmarkItemResult>>& result, const NetworkBenchmarkParameters& parameters) {
  Aws::StringStream benchmark_name;
  benchmark_name << "NetworkLatencyBenchmark/";
  benchmark_name << magic_enum::enum_name(execute_mode_) << "/" << parameters.function_instance_mb_size_ << "MB";

  const auto get_latency_aggregates =
      BenchmarkHelper::CalculateAggregates(result, [&](const BenchmarkItemResult& single_result) {
        return BenchmarkHelper::ExtractMetric(single_result, kJsonGetObjectDurationKey);
      });
  const auto put_latency_aggregates =
      BenchmarkHelper::CalculateAggregates(result, [&](const BenchmarkItemResult& single_result) {
        return BenchmarkHelper::ExtractMetric(single_result, kJsonPutObjectDurationKey);
      });

  return BenchmarkHelper::GenerateJsonOutput(
      benchmark_name.str(),
      {{"get_object_latency_average", get_latency_aggregates.average},
       {"get_object_latency_minimum", get_latency_aggregates.minimum},
       {"get_object_latency_median", get_latency_aggregates.median},
       {"get_object_latency_maximum", get_latency_aggregates.maximum},
       {"get_object_latency_percentile_90", get_latency_aggregates.percentile_90},
       {"get_object_latency_percentile_99", get_latency_aggregates.percentile_99},
       {"get_object_latency_percentile_99.9", get_latency_aggregates.percentile_99_9},
       {"get_object_latency_percentile_99.99", get_latency_aggregates.percentile_99_99},
       {"get_object_latency_std_dev", get_latency_aggregates.standard_deviation},
       {"put_object_latency_average", put_latency_aggregates.average},
       {"put_object_latency_minimum", put_latency_aggregates.minimum},
       {"put_object_latency_median", put_latency_aggregates.median},
       {"put_object_latency_maximum", put_latency_aggregates.maximum},
       {"put_object_latency_percentile_90", put_latency_aggregates.percentile_90},
       {"put_object_latency_percentile_99", put_latency_aggregates.percentile_99},
       {"put_object_latency_percentile_99.9", put_latency_aggregates.percentile_99_9},
       {"put_object_latency_percentile_99.99", put_latency_aggregates.percentile_99_99},
       {"put_object_latency_std_dev", put_latency_aggregates.standard_deviation},
       {"benchmark_cost_usd", CalculateBenchmarkCost(result, parameters.function_instance_mb_size_)},
       {"benchmark_cost_overhead_usd", cost_overhead_ / configs_.size()}},
      {/*aggregated string metrics*/}, result,
      {[&](const BenchmarkItemResult& single_result) {
         return std::make_tuple("get_object_latency_ms",
                                BenchmarkHelper::ExtractMetric(single_result, kJsonGetObjectDurationKey));
       },
       [&](const BenchmarkItemResult& single_result) {
         return std::make_tuple("put_object_latency_ms",
                                BenchmarkHelper::ExtractMetric(single_result, kJsonPutObjectDurationKey));
       },
       [&](const BenchmarkItemResult& single_result) {
         return std::make_tuple("billed_lambda_duration_ms",
                                BenchmarkHelper::ExtractBilledLambdaDuration(single_result));
       },
       [&](const BenchmarkItemResult& single_result) {
         return std::make_tuple("function_cost_usd",
                                ExtractFunctionCost(single_result, parameters.function_instance_mb_size_));
       }},
      {/*extract string metric functions*/});
}

}  // namespace skyrise
