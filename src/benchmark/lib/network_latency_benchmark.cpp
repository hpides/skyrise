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
  benchmark_name << magic_enum::enum_name(execute_mode_) << "/" << parameters.function_instance_mb_size_
                 << "FunctionInstanceMB/";
  benchmark_name << magic_enum::enum_name(parameters.operation_type_);

  const auto aggregates = BenchmarkHelper::CalculateAggregates(result, [&](const BenchmarkItemResult& single_result) {
    return BenchmarkHelper::ExtractMetric(single_result, "duration_ms");
  });

  return BenchmarkHelper::GenerateJsonOutput(
      benchmark_name.str(),
      {{"latency_ms_average", aggregates.average},
       {"latency_ms_minimum", aggregates.minimum},
       {"latency_ms_median", aggregates.median},
       {"latency_ms_maximum", aggregates.maximum},
       {"latency_ms_percentile_90", aggregates.percentile_90},
       {"latency_ms_percentile_99", aggregates.percentile_99},
       {"latency_ms_percentile_99.9", aggregates.percentile_99_9},
       {"latency_ms_percentile_99.99", aggregates.percentile_99_99},
       {"latency_ms_std_dev", aggregates.standard_deviation},
       {"benchmark_cost_usd", CalculateBenchmarkCost(result, parameters.function_instance_mb_size_)},
       {"benchmark_cost_overhead_usd", cost_overhead_ / configs_.size()}},
      {/*aggregated string metrics*/}, result,
      {[&](const BenchmarkItemResult& single_result) {
         return std::make_tuple("latency_ms", BenchmarkHelper::ExtractMetric(single_result, "duration_ms"));
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
