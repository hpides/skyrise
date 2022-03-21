#include "lambda_benchmark.hpp"

#include <algorithm>
#include <numeric>

#include "utils/assert.hpp"
#include "utils/string.hpp"

namespace skyrise {

LambdaBenchmark::LambdaBenchmark(std::shared_ptr<const CostCalculator> cost_calculator)
    : cost_calculator_(std::move(cost_calculator)) {}

Aws::Utils::Array<Aws::Utils::Json::JsonValue> LambdaBenchmark::Run(
    const std::shared_ptr<AbstractBenchmarkRunner>& benchmark_runner) {
  const auto lambda_benchmark_runner = std::dynamic_pointer_cast<LambdaBenchmarkRunner>(benchmark_runner);
  Assert(lambda_benchmark_runner, "LambdaBenchmark needs a LambdaBenchmarkRunner to run.");
  return OnRun(lambda_benchmark_runner);
}

long double LambdaBenchmark::CalculateOverallFunctionCost(
    const std::shared_ptr<LambdaBenchmarkResult>& benchmark_result, const size_t function_instance_mb_size,
    const bool is_provisioned_concurrency) const {
  const auto& benchmark_repetitions = benchmark_result->GetBenchmarkRepetitions();

  std::vector<long double> function_costs;
  function_costs.reserve(benchmark_repetitions.size() * benchmark_repetitions.front().GetInvokeResults().size());

  for (const auto& benchmark_repetition : benchmark_repetitions) {
    const auto& invoke_results = benchmark_repetition.GetInvokeResults();

    std::transform(invoke_results.cbegin(), invoke_results.cend(), std::back_inserter(function_costs),
                   [&](const LambdaInvokeResult& invoke_result) {
                     return ExtractFunctionCost(invoke_result, function_instance_mb_size, is_provisioned_concurrency);
                   });
  }

  const long double benchmark_cost = std::accumulate(function_costs.cbegin(), function_costs.cend(), 0.0L);

  return benchmark_cost;
}

long double LambdaBenchmark::ExtractFunctionCost(const LambdaInvokeResult& invoke_result,
                                                 const size_t function_instance_mb_size,
                                                 const bool is_provisioned_concurrency) const {
  const double billed_duration =
      invoke_result.HasLogResult() ? invoke_result.GetLogResult()->GetBilledDurationMs() : 0.0;

  const long double function_instance_cost =
      cost_calculator_->CalculateCostLambda(billed_duration, function_instance_mb_size, is_provisioned_concurrency);

  const auto response_body = invoke_result.GetResponseBody();

  const size_t num_s3_requests_tier_1 =
      response_body.KeyExists("num_s3_requests_tier_1") ? response_body.GetInteger("num_s3_requests_tier_1") : 0;
  const size_t num_s3_requests_tier_2 =
      response_body.KeyExists("num_s3_requests_tier_2") ? response_body.GetInteger("num_s3_requests_tier_2") : 0;
  const size_t s3_storage_used_bytes =
      response_body.KeyExists("s3_storage_used_bytes") ? response_body.GetInt64("s3_storage_used_bytes") : 0;

  const long double s3_request_cost =
      cost_calculator_->CalculateCostS3Requests(num_s3_requests_tier_1, num_s3_requests_tier_2);

  // TODO(d-justen): Find a way to track actual hours. For now, we assume that S3 object in this benchmark will be
  // deleted within an hour.
  const long double s3_storage_cost = cost_calculator_->CalculateCostS3StorageMonthly(s3_storage_used_bytes, 1);

  return function_instance_cost + s3_request_cost + s3_storage_cost;
}

}  // namespace skyrise
