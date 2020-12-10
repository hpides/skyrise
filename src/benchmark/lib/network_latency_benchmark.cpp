#include "network_latency_benchmark.hpp"

#include <algorithm>
#include <numeric>

#include <magic_enum.hpp>

#include "utils/costs/pricing.hpp"

namespace skyrise {

NetworkLatencyBenchmark::NetworkLatencyBenchmark(std::shared_ptr<BenchmarkHelper> helper,
                                                 std::shared_ptr<CostCalculator> cost_calculator,
                                                 const std::vector<size_t>& function_instance_mb_sizes,
                                                 const size_t num_iterations)
    : NetworkBenchmark(std::move(helper), std::move(cost_calculator), ExecuteMode::kWarmSequential, num_iterations) {
  for (const auto function_instance_mb_size : function_instance_mb_sizes) {
    for (const auto operation_type : {S3OperationType::kRead, S3OperationType::kWrite}) {
      Aws::StringStream function_name;
      function_name << "skyriseFunction" << (operation_type == S3OperationType::kRead ? "Read" : "Write") << "S3";

      BenchmarkConfig config(function_name.str(), function_instance_mb_size, num_iterations_, execute_mode_);
      config.SetPayloads(
          GeneratePayloads(function_instance_mb_size, kObjectBytesSize, 1, operation_type, num_iterations_));
      configs_.emplace_back(config,
                            NetworkBenchmarkParameters{function_instance_mb_size, kObjectBytesSize, 1, operation_type});
    }
  }
}

Aws::Utils::Json::JsonValue NetworkLatencyBenchmark::GenerateResultOutput(
    const std::shared_ptr<std::vector<BenchmarkItemResult>>& result, const NetworkBenchmarkParameters& parameters) {
  Aws::StringStream benchmark_name;
  benchmark_name << "NetworkLatencyBenchmark/" << parameters.function_instance_mb_size_ << "FunctionInstanceMB/"
                 << magic_enum::enum_name(parameters.operation_type_);

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
