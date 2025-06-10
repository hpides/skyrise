#include "lambda_benchmark_output.hpp"

namespace skyrise {

LambdaBenchmarkOutput::LambdaBenchmarkOutput(Aws::String name, const LambdaBenchmarkParameters& parameters,
                                             std::shared_ptr<LambdaBenchmarkResult> result)
    : BenchmarkOutput<LambdaBenchmarkParameters, LambdaBenchmarkResult, LambdaBenchmarkRepetitionResult,
                      LambdaInvocationResult>(std::move(name), parameters, std::move(result)) {
  metrics_.WithDouble("lambda_runtime_duration_ms", benchmark_result_->GetAccumulatedLambdaDurationMs());
  const double warmup_cost_usd = benchmark_result_->CalculateWarmupCost();
  const double runtime_cost_usd =
      benchmark_result_->CalculateRuntimeCost(benchmark_parameters_.function_instance_size_mb);
  const double trace_cost_usd = benchmark_result_->GetTraceCost();
  metrics_.WithDouble("lambda_total_cost_usd", warmup_cost_usd + runtime_cost_usd + trace_cost_usd);
  metrics_.WithDouble("lambda_warmup_cost_usd", warmup_cost_usd);
  metrics_.WithDouble("lambda_runtime_cost_usd", runtime_cost_usd);
  metrics_.WithDouble("lambda_trace_cost_usd", trace_cost_usd);
}

Aws::Utils::Json::JsonValue LambdaBenchmarkOutput::Build() const {
  auto benchmark_output =
      Aws::Utils::Json::JsonValue().WithObject("parameters", parameters_).WithObject("metrics", metrics_);

  const auto& benchmark_repetitions = benchmark_result_->GetRepetitionResults();
  const std::shared_ptr<const CostCalculator> cost_calculator = benchmark_result_->GetCostCalculator();

  Aws::Utils::Array<Aws::Utils::Json::JsonValue> repetitions(benchmark_repetitions.size());

  for (size_t i = 0; i < benchmark_repetitions.size(); ++i) {
    const auto& repetition_result = benchmark_repetitions[i];
    const double repetition_warmup_cost_usd = repetition_result.GetWarmupCost();
    const double repetition_runtime_cost_usd =
        repetition_result.CalculateRuntimeCost(cost_calculator, benchmark_parameters_.function_instance_size_mb);
    auto repetition_result_value =
        Aws::Utils::Json::JsonValue()
            .WithString("repetition_end_timestamp", repetition_result.GetEndTimestamp())
            .WithDouble("repetition_duration_ms", repetition_result.GetDurationMs())
            .WithDouble("repetition_lambda_runtime_duration_ms", repetition_result.GetAccumulatedLambdaDurationMs())
            .WithDouble("repetition_lambda_total_cost_usd", repetition_warmup_cost_usd + repetition_runtime_cost_usd)
            .WithDouble("repetition_lambda_warmup_cost_usd", repetition_warmup_cost_usd)
            .WithDouble("repetition_lambda_runtime_cost_usd", repetition_runtime_cost_usd);

    repetition_functors_.AppendMetrics(repetition_result, repetition_result_value);

    Aws::Utils::Array<Aws::Utils::Json::JsonValue> invocations(benchmark_repetitions[i].GetInvocationResults().size());

    for (size_t j = 0; j < benchmark_repetitions[i].GetInvocationResults().size(); ++j) {
      const auto& invocation_result = benchmark_repetitions[i].GetInvocationResults()[j];

      auto invocation_result_value =
          Aws::Utils::Json::JsonValue()
              .WithString("invocation_id", invocation_result.GetInstanceId())
              .WithBool("succeeded", invocation_result.IsSuccess())
              .WithDouble("invocation_total_duration_ms", invocation_result.GetDurationMs())
              .WithDouble("invocation_billed_duration_ms", invocation_result.HasResultLog()
                                                               ? invocation_result.GetResultLog()->GetBilledDurationMs()
                                                               : 0.0)
              .WithInteger("invocation_sleep_duration_seconds", invocation_result.GetSleepDurationSeconds())
              .WithDouble("invocation_cost_usd", invocation_result.CalculateCost(
                                                     cost_calculator, benchmark_parameters_.function_instance_size_mb))
              .WithObject("introspection",
                          invocation_result.GetResponseBody().KeyExists("introspection")
                              ? invocation_result.GetResponseBody().GetObject("introspection").WriteCompact()
                              : "")
              .WithObject(kResponseMeteringAttribute,
                          invocation_result.GetResponseBody().KeyExists(kResponseMeteringAttribute)
                              ? invocation_result.GetResponseBody().GetObject(kResponseMeteringAttribute).WriteCompact()
                              : "");

      if (invocation_result.IsSuccess()) {
        invocation_functors_.AppendMetrics(invocation_result, invocation_result_value);
      }

      invocations[j] = invocation_result_value;
    }

    repetitions[i] = repetition_result_value.WithArray("invocations", invocations);
  }

  benchmark_output.WithArray("repetitions", repetitions);

  return benchmark_output;
}

double LambdaBenchmarkOutput::GetLambdaRuntimeCosts() const {
  Assert(metrics_.View().KeyExists("lambda_runtime_cost_usd"), "Total Lambda costs are not available.");
  return metrics_.View().GetDouble("lambda_runtime_cost_usd");
}

}  // namespace skyrise
