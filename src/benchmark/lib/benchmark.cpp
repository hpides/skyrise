#include "benchmark.hpp"

#include <algorithm>
#include <numeric>

#include "utils/assert.hpp"
#include "utils/string.hpp"

namespace skyrise {

Benchmark::Benchmark(std::shared_ptr<CostCalculator> cost_calculator) : cost_calculator_(std::move(cost_calculator)) {}

long double Benchmark::CalculateOverallFunctionCost(const std::shared_ptr<BenchmarkResult>& benchmark_result,
                                                    const size_t function_instance_mb_size,
                                                    const bool is_provisioned_concurrency) const {
  const auto& benchmark_repetitions = benchmark_result->GetBenchmarkRepetitions();

  std::vector<long double> function_costs;
  function_costs.reserve(benchmark_repetitions.size() * benchmark_repetitions.front().GetInvokeResults().size());

  for (const auto& benchmark_repetition : benchmark_repetitions) {
    const auto& invoke_results = benchmark_repetition.GetInvokeResults();

    std::transform(invoke_results.cbegin(), invoke_results.cend(), std::back_inserter(function_costs),
                   [&](const InvokeResult& invoke_result) {
                     return ExtractFunctionCost(invoke_result, function_instance_mb_size, is_provisioned_concurrency);
                   });
  }

  const long double benchmark_cost = std::accumulate(function_costs.cbegin(), function_costs.cend(), 0.0L);

  return benchmark_cost;
}

long double Benchmark::ExtractFunctionCost(const InvokeResult& invoke_result, const size_t function_instance_mb_size,
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

Aws::Utils::Json::JsonValue Benchmark::GenerateJsonOutput(
    const Aws::String& benchmark_name, const std::vector<std::tuple<Aws::String, double>>& aggregated_numeric_metrics,
    const std::vector<std::tuple<Aws::String, Aws::String>>& aggregated_alphabetic_metrics,
    const std::shared_ptr<BenchmarkResult>& benchmark_result,
    const std::vector<std::function<std::tuple<Aws::String, double>(const InvokeResult&)>>&
        extract_numeric_metric_functions,
    const std::vector<std::function<std::tuple<Aws::String, Aws::String>(const InvokeResult&)>>&
        extract_alphabetic_metric_functions,
    const std::vector<std::function<std::tuple<Aws::String, Aws::Utils::Json::JsonValue>(const InvokeResult&)>>&
        extract_object_metric_functions) {
  auto json_output = Aws::Utils::Json::JsonValue().WithString("name", benchmark_name);

  for (const auto& [metric_name, aggregated_numeric_metric] : aggregated_numeric_metrics) {
    json_output = json_output.WithDouble(metric_name, aggregated_numeric_metric);
  }

  for (const auto& [metric_name, aggregated_alphabetic_metric] : aggregated_alphabetic_metrics) {
    json_output = json_output.WithString(metric_name, aggregated_alphabetic_metric);
  }

  const auto& benchmark_repetitions = benchmark_result->GetBenchmarkRepetitions();

  Aws::Utils::Array<Aws::Utils::Json::JsonValue> repetitions(benchmark_repetitions.size());

  for (size_t i = 0; i < benchmark_repetitions.size(); i++) {
    auto repetition_value =
        Aws::Utils::Json::JsonValue()
            .WithInteger("repetition", i)
            .WithDouble("duration_ms", benchmark_repetitions[i].GetDurationMs())
            .WithDouble("warmup_cost_usd", static_cast<double>(benchmark_repetitions[i].GetWarmUpCost()));

    Aws::Utils::Array<Aws::Utils::Json::JsonValue> invocations(benchmark_repetitions[i].GetInvokeResults().size());

    size_t j = 0;
    for (const auto& invoke_result : benchmark_repetitions[i].GetInvokeResults()) {
      auto invoke_result_value = Aws::Utils::Json::JsonValue().WithString("name", invoke_result.GetInvokeId());

      if (invoke_result.IsSuccess()) {
        invoke_result_value = invoke_result_value.WithBool("success", true);

        for (const auto& extract_numeric_metric_function : extract_numeric_metric_functions) {
          const auto& [metric_name, numeric_metric] = extract_numeric_metric_function(invoke_result);
          invoke_result_value = invoke_result_value.WithDouble(metric_name, numeric_metric);
        }

        for (const auto& extract_alphabetic_metric_function : extract_alphabetic_metric_functions) {
          const auto& [metric_name, alphabetic_metric] = extract_alphabetic_metric_function(invoke_result);
          invoke_result_value = invoke_result_value.WithString(metric_name, alphabetic_metric);
        }

        for (const auto& extract_object_metric_function : extract_object_metric_functions) {
          const auto& [metric_name, object_metric] = extract_object_metric_function(invoke_result);
          invoke_result_value = invoke_result_value.WithObject(metric_name, object_metric);
        }
      } else {
        invoke_result_value = invoke_result_value.WithBool("success", false);
      }

      invocations[j] = invoke_result_value;
      j++;
    }

    repetitions[i] = repetition_value.WithArray("invocations", invocations);
  }

  json_output = json_output.WithArray("repetitions", repetitions);

  return json_output;
}

}  // namespace skyrise
