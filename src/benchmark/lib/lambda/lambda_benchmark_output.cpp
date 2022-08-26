#include "lambda_benchmark_output.hpp"

namespace skyrise {

template <class T>
Aws::Utils::Json::JsonValue& MetricFunctors<T>::AppendMetrics(T result, Aws::Utils::Json::JsonValue& output) const {
  {
    for (const auto& bool_functor : bool_functors) {
      const auto& [name, value] = bool_functor(result);
      output.WithBool(name, value);
    }

    for (const auto& int64_functor : int64_functors) {
      const auto& [name, value] = int64_functor(result);
      output.WithInt64(name, value);
    }

    for (const auto& double_functor : double_functors) {
      const auto& [name, value] = double_functor(result);
      output.WithDouble(name, value);
    }

    for (const auto& string_functor : string_functors) {
      const auto& [name, value] = string_functor(result);
      output.WithString(name, value);
    }

    for (const auto& object_functor : object_functors) {
      const auto& [name, value] = object_functor(result);
      output.WithObject(name, value);
    }

    return output;
  }
}

LambdaBenchmarkOutput::LambdaBenchmarkOutput(Aws::String benchmark_name,
                                             std::shared_ptr<LambdaBenchmarkResult> benchmark_result)
    : benchmark_name_(std::move(benchmark_name)), benchmark_result_(std::move(benchmark_result)) {
  metrics_.WithDouble("benchmark_duration_ms", benchmark_result_->GetDurationMs());
}

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
  invocation_functors_.bool_functors.push_back(std::move(functor));
  return *this;
}

LambdaBenchmarkOutput& LambdaBenchmarkOutput::WithInt64InvocationMetric(
    std::function<std::tuple<Aws::String, long long>(const LambdaInvokeResult&)> functor) {
  invocation_functors_.int64_functors.push_back(std::move(functor));
  return *this;
}

LambdaBenchmarkOutput& LambdaBenchmarkOutput::WithDoubleInvocationMetric(
    std::function<std::tuple<Aws::String, double>(const LambdaInvokeResult&)> functor) {
  invocation_functors_.double_functors.push_back(std::move(functor));
  return *this;
}

LambdaBenchmarkOutput& LambdaBenchmarkOutput::WithStringInvocationMetric(
    std::function<std::tuple<Aws::String, Aws::String>(const LambdaInvokeResult&)> functor) {
  invocation_functors_.string_functors.push_back(std::move(functor));
  return *this;
}

LambdaBenchmarkOutput& LambdaBenchmarkOutput::WithObjectInvocationMetric(
    std::function<std::tuple<Aws::String, Aws::Utils::Json::JsonValue>(const LambdaInvokeResult&)> functor) {
  invocation_functors_.object_functors.push_back(std::move(functor));
  return *this;
}

LambdaBenchmarkOutput& LambdaBenchmarkOutput::WithBoolRepetitionMetric(
    std::function<std::tuple<Aws::String, bool>(const LambdaBenchmarkRepetition&)> functor) {
  repetition_functors_.bool_functors.push_back(std::move(functor));
  return *this;
}

LambdaBenchmarkOutput& LambdaBenchmarkOutput::WithInt64RepetitionMetric(
    std::function<std::tuple<Aws::String, long long>(const LambdaBenchmarkRepetition&)> functor) {
  repetition_functors_.int64_functors.push_back(std::move(functor));
  return *this;
}

LambdaBenchmarkOutput& LambdaBenchmarkOutput::WithDoubleRepetitionMetric(
    std::function<std::tuple<Aws::String, double>(const LambdaBenchmarkRepetition&)> functor) {
  repetition_functors_.double_functors.push_back(std::move(functor));
  return *this;
}

LambdaBenchmarkOutput& LambdaBenchmarkOutput::WithStringRepetitionMetric(
    std::function<std::tuple<Aws::String, Aws::String>(const LambdaBenchmarkRepetition&)> functor) {
  repetition_functors_.string_functors.push_back(std::move(functor));
  return *this;
}

LambdaBenchmarkOutput& LambdaBenchmarkOutput::WithObjectRepetitionMetric(
    std::function<std::tuple<Aws::String, Aws::Utils::Json::JsonValue>(const LambdaBenchmarkRepetition&)> functor) {
  repetition_functors_.object_functors.push_back(std::move(functor));
  return *this;
}

Aws::Utils::Json::JsonValue LambdaBenchmarkOutput::Build() const {
  auto benchmark_output =
      Aws::Utils::Json::JsonValue().WithObject("arguments", arguments_).WithObject("metrics", metrics_);

  const auto& benchmark_repetitions = benchmark_result_->GetBenchmarkRepetitions();

  Aws::Utils::Array<Aws::Utils::Json::JsonValue> repetitions(benchmark_repetitions.size());

  for (size_t i = 0; i < benchmark_repetitions.size(); ++i) {
    const auto& repetition = benchmark_repetitions[i];
    auto repetition_value =
        Aws::Utils::Json::JsonValue()
            .WithDouble("repetition_duration_ms", repetition.GetDurationMs())
            .WithDouble("repetition_warmup_cost_usd", static_cast<double>(repetition.GetWarmUpCost()));

    repetition_functors_.AppendMetrics(repetition, repetition_value);

    Aws::Utils::Array<Aws::Utils::Json::JsonValue> invocations(benchmark_repetitions[i].GetInvokeResults().size());

    for (size_t j = 0; j < benchmark_repetitions[i].GetInvokeResults().size(); ++j) {
      const auto& invoke_result = benchmark_repetitions[i].GetInvokeResults()[j];

      auto invoke_result_value = Aws::Utils::Json::JsonValue()
                                     .WithString("name", invoke_result.GetInvokeId())
                                     .WithBool("success", invoke_result.IsSuccess())
                                     .WithDouble("invocation_duration_ms", invoke_result.GetDurationMs());

      if (invoke_result.IsSuccess()) {
        invocation_functors_.AppendMetrics(invoke_result, invoke_result_value);
      }

      invocations[j] = invoke_result_value;
    }

    repetitions[i] = repetition_value.WithArray("invocations", invocations);
  }

  benchmark_output.WithArray("repetitions", repetitions);

  return benchmark_output;
}

}  // namespace skyrise
