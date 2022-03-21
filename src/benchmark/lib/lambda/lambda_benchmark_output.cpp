#include "lambda_benchmark_output.hpp"

namespace skyrise {

LambdaBenchmarkOutput::LambdaBenchmarkOutput(Aws::String benchmark_name,
                                             std::shared_ptr<LambdaBenchmarkResult> benchmark_result)
    : benchmark_name_(std::move(benchmark_name)), benchmark_result_(std::move(benchmark_result)) {}

LambdaBenchmarkOutput& LambdaBenchmarkOutput::WithBoolArgument(const Aws::String& name, bool value) {
  arguments_.WithBool(name, value);
  return *this;
}

LambdaBenchmarkOutput& LambdaBenchmarkOutput::WithInt64Argument(const Aws::String& name, long long value) {
  arguments_.WithInt64(name, value);
  return *this;
}

LambdaBenchmarkOutput& LambdaBenchmarkOutput::WithDoubleArgument(const Aws::String& name, double value) {
  arguments_.WithDouble(name, value);
  return *this;
}

LambdaBenchmarkOutput& LambdaBenchmarkOutput::WithStringArgument(const Aws::String& name, const Aws::String& value) {
  arguments_.WithString(name, value);
  return *this;
}

LambdaBenchmarkOutput& LambdaBenchmarkOutput::WithBoolMetric(const Aws::String& name, bool value) {
  metrics_.WithBool(name, value);
  return *this;
}

LambdaBenchmarkOutput& LambdaBenchmarkOutput::WithInt64Metric(const Aws::String& name, long long value) {
  metrics_.WithInt64(name, value);
  return *this;
}

LambdaBenchmarkOutput& LambdaBenchmarkOutput::WithDoubleMetric(const Aws::String& name, double value) {
  metrics_.WithDouble(name, value);
  return *this;
}

LambdaBenchmarkOutput& LambdaBenchmarkOutput::WithStringMetric(const Aws::String& name, const Aws::String& value) {
  metrics_.WithString(name, value);
  return *this;
}

LambdaBenchmarkOutput& LambdaBenchmarkOutput::WithBoolInvocationMetric(
    std::function<std::tuple<Aws::String, bool>(const LambdaInvokeResult&)> functor) {
  bool_invocation_functors_.push_back(std::move(functor));
  return *this;
}

LambdaBenchmarkOutput& LambdaBenchmarkOutput::WithInt64InvocationMetric(
    std::function<std::tuple<Aws::String, long long>(const LambdaInvokeResult&)> functor) {
  int64_invocation_functors_.push_back(std::move(functor));
  return *this;
}

LambdaBenchmarkOutput& LambdaBenchmarkOutput::WithDoubleInvocationMetric(
    std::function<std::tuple<Aws::String, double>(const LambdaInvokeResult&)> functor) {
  double_invocation_functors_.push_back(std::move(functor));
  return *this;
}

LambdaBenchmarkOutput& LambdaBenchmarkOutput::WithStringInvocationMetric(
    std::function<std::tuple<Aws::String, Aws::String>(const LambdaInvokeResult&)> functor) {
  string_invocation_functors_.push_back(std::move(functor));
  return *this;
}

LambdaBenchmarkOutput& LambdaBenchmarkOutput::WithObjectInvocationMetric(
    std::function<std::tuple<Aws::String, Aws::Utils::Json::JsonValue>(const LambdaInvokeResult&)> functor) {
  object_invocation_functors_.push_back(std::move(functor));
  return *this;
}

Aws::Utils::Json::JsonValue LambdaBenchmarkOutput::Build() const {
  auto benchmark_output = Aws::Utils::Json::JsonValue()
                              .WithString("name", benchmark_name_)
                              .WithObject("arguments", arguments_)
                              .WithObject("metrics", metrics_);

  const auto& benchmark_repetitions = benchmark_result_->GetBenchmarkRepetitions();

  Aws::Utils::Array<Aws::Utils::Json::JsonValue> repetitions(benchmark_repetitions.size());

  for (size_t i = 0; i < benchmark_repetitions.size(); ++i) {
    auto repetition_value =
        Aws::Utils::Json::JsonValue()
            .WithInteger("repetition", i)
            .WithDouble("duration_ms", benchmark_repetitions[i].GetDurationMs())
            .WithDouble("warmup_cost_usd", static_cast<double>(benchmark_repetitions[i].GetWarmUpCost()));

    Aws::Utils::Array<Aws::Utils::Json::JsonValue> invocations(benchmark_repetitions[i].GetInvokeResults().size());

    for (size_t j = 0; j < benchmark_repetitions[i].GetInvokeResults().size(); ++j) {
      const auto& invoke_result = benchmark_repetitions[i].GetInvokeResults()[j];

      auto invoke_result_value = Aws::Utils::Json::JsonValue()
                                     .WithString("name", invoke_result.GetInvokeId())
                                     .WithBool("success", invoke_result.IsSuccess());

      if (invoke_result.IsSuccess()) {
        for (const auto& bool_functor : bool_invocation_functors_) {
          const auto& [name, value] = bool_functor(invoke_result);
          invoke_result_value.WithBool(name, value);
        }

        for (const auto& int64_functor : int64_invocation_functors_) {
          const auto& [name, value] = int64_functor(invoke_result);
          invoke_result_value.WithInt64(name, value);
        }

        for (const auto& double_functor : double_invocation_functors_) {
          const auto& [name, value] = double_functor(invoke_result);
          invoke_result_value.WithDouble(name, value);
        }

        for (const auto& string_functor : string_invocation_functors_) {
          const auto& [name, value] = string_functor(invoke_result);
          invoke_result_value.WithString(name, value);
        }

        for (const auto& object_functor : object_invocation_functors_) {
          const auto& [name, value] = object_functor(invoke_result);
          invoke_result_value.WithObject(name, value);
        }
      }

      invocations[j] = invoke_result_value;
    }

    repetitions[i] = repetition_value.WithArray("invocations", invocations);
  }

  benchmark_output.WithArray("repetitions", repetitions);

  return benchmark_output;
}

}  // namespace skyrise
