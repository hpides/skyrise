#include "ec2_benchmark_output.hpp"

namespace skyrise {

Ec2BenchmarkOutput::Ec2BenchmarkOutput(Aws::String name, const Ec2BenchmarkParameters& parameters,
                                       std::shared_ptr<Ec2BenchmarkResult> result)
    : BenchmarkOutput(std::move(name), parameters, std::move(result)) {
  client_ = std::make_shared<CoordinatorClient>();
  cost_calculator_ = std::make_shared<CostCalculator>(client_->GetPricingClient(), client_->GetClientRegion());
  const double warmstart_cost_usd = benchmark_result_->CalculateWarmstartCost(cost_calculator_);
  const double coldstart_cost_usd = benchmark_result_->CalculateColdstartCost(cost_calculator_);
  metrics_.WithDouble("ec2_total_cost_usd", warmstart_cost_usd + coldstart_cost_usd);
  metrics_.WithDouble("ec2_warmup_cost_usd", warmstart_cost_usd);
  metrics_.WithDouble("ec2_coldstart_cost_usd", coldstart_cost_usd);
  metrics_.WithDouble("ec2_billed_duration_ms", benchmark_result_->GetBilledDurationMs());
  metrics_.WithDouble("benchmark_duration_ms", benchmark_result_->GetDurationMs());
}

Aws::Utils::Json::JsonValue Ec2BenchmarkOutput::Build() const {
  auto benchmark_output =
      Aws::Utils::Json::JsonValue().WithObject("parameters", parameters_).WithObject("metrics", metrics_);

  const auto& benchmark_repetitions = benchmark_result_->GetRepetitionResults();

  Aws::Utils::Array<Aws::Utils::Json::JsonValue> repetitions(benchmark_repetitions.size());

  for (size_t i = 0; i < benchmark_repetitions.size(); ++i) {
    const auto& repetition = benchmark_repetitions[i];
    double warmstart_cost_usd = benchmark_result_->GetRepetitionWarmstartCost(i);
    double coldstart_cost_usd = benchmark_result_->GetRepetitionColdstartCost(i);
    auto repetition_value = Aws::Utils::Json::JsonValue()
                                .WithDouble("repetition_duration_ms", repetition.duration_ms.value())
                                .WithDouble("repetition_ec2_total_cost_usd", warmstart_cost_usd + coldstart_cost_usd)
                                .WithDouble("repetition_ec2_warmstart_cost_usd", warmstart_cost_usd)
                                .WithDouble("repetition_ec2_coldstart_cost_usd", coldstart_cost_usd);

    repetition_functors_.AppendMetrics(repetition, repetition_value);

    Aws::Utils::Array<Aws::Utils::Json::JsonValue> invocations(repetition.invocation_results.size());

    size_t j = 0;
    for (const auto& [s, invocation] : repetition.invocation_results) {
      auto invoke_result_value = Aws::Utils::Json::JsonValue().WithDouble(
          "coldstart_duration_ms", invocation.instance_transitions.coldstart_duration_ms);

      if (invocation.instance_transitions.warmstart_duration_ms.has_value()) {
        invoke_result_value
            .WithDouble("warmstart_duration_ms", invocation.instance_transitions.warmstart_duration_ms.value())
            .WithDouble("stop_duration_ms", invocation.instance_transitions.stop_duration_ms.value());
      }

      invoke_result_value.WithDouble("termination_duration_ms",
                                     invocation.instance_transitions.termination_duration_ms.value());

      invocation_functors_.AppendMetrics(invocation, invoke_result_value);

      invocations[j] = invoke_result_value;
      j++;
    }

    repetitions[i] = repetition_value.WithArray("invocations", invocations);
  }
  benchmark_output.WithArray("repetitions", repetitions);

  return benchmark_output;
}

}  // namespace skyrise
