#include "benchmark.hpp"

#include <algorithm>
#include <numeric>

#include "benchmark_helper.hpp"
#include "utils/string.hpp"

namespace skyrise {

Benchmark::Benchmark(std::shared_ptr<CostCalculator> cost_calculator) : cost_calculator_(std::move(cost_calculator)) {}

long double Benchmark::CalculateOverallFunctionCost(const std::shared_ptr<BenchmarkResult>& result,
                                                    const size_t function_instance_mb_size,
                                                    const bool is_provisioned_concurrency) const {
  const auto invocation_results = result->GetInvocationResults();

  std::vector<long double> function_costs;
  function_costs.reserve(invocation_results.size() * invocation_results.front().size());

  for (const auto& repetition : invocation_results) {
    std::transform(repetition.cbegin(), repetition.cend(), std::back_inserter(function_costs),
                   [&](const std::pair<Aws::String, InvocationResult>& map_entry) {
                     return ExtractFunctionCost(map_entry.second, function_instance_mb_size,
                                                is_provisioned_concurrency);
                   });
  }

  const long double benchmark_cost = std::accumulate(function_costs.cbegin(), function_costs.cend(), 0.0L);

  return benchmark_cost;
}

long double Benchmark::ExtractFunctionCost(const InvocationResult& result, const size_t function_instance_mb_size,
                                           const bool is_provisioned_concurrency) const {
  const double billed_duration = BenchmarkHelper::ExtractLogResultMetric(result, "Billed Duration").value_or(0.0);
  const long double function_instance_cost =
      cost_calculator_->CalculateCostLambda(billed_duration, function_instance_mb_size, is_provisioned_concurrency);

  const auto payload_value = Aws::Utils::Json::JsonValue(StreamToString(&result.invoke_result->GetPayload()));
  const auto payload_view = payload_value.View();

  const size_t num_s3_requests_tier_1 =
      payload_view.KeyExists("num_s3_requests_tier_1") ? payload_view.GetInteger("num_s3_requests_tier_1") : 0;
  const size_t num_s3_requests_tier_2 =
      payload_view.KeyExists("num_s3_requests_tier_2") ? payload_view.GetInteger("num_s3_requests_tier_2") : 0;
  const size_t s3_storage_used_bytes =
      payload_view.KeyExists("s3_storage_used_bytes") ? payload_view.GetInt64("s3_storage_used_bytes") : 0;

  const long double s3_request_cost =
      cost_calculator_->CalculateCostS3Requests(num_s3_requests_tier_1, num_s3_requests_tier_2);

  // TODO(d-justen): Find a way to track actual hours. For now, we assume that S3 object in this benchmark will be
  // deleted within an hour.
  const long double s3_storage_cost = cost_calculator_->CalculateCostS3StorageMonthly(s3_storage_used_bytes, 1);

  return function_instance_cost + s3_request_cost + s3_storage_cost;
}

}  // namespace skyrise
