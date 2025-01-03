#include "benchmark_output.hpp"

namespace skyrise {

template <class T>
Aws::Utils::Json::JsonValue& MetricFunctors<T>::AppendMetrics(T result, Aws::Utils::Json::JsonValue& output) const {
  for (const auto& bool_functor : bool_functors) {
    const auto& [name, value] = bool_functor(result);
    output.WithBool(name, value);
  }

  for (const auto& double_functor : double_functors) {
    const auto& [name, value] = double_functor(result);
    output.WithDouble(name, value);
  }

  for (const auto& int64_functor : int64_functors) {
    const auto& [name, value] = int64_functor(result);
    output.WithInt64(name, value);
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

template <typename Parameters, typename BenchmarkResult, typename RepetitionResult, typename InvocationResult>
BenchmarkOutput<Parameters, BenchmarkResult, RepetitionResult, InvocationResult>::BenchmarkOutput(
    Aws::String name, const Parameters& parameters, std::shared_ptr<BenchmarkResult> result)
    : benchmark_name_(std::move(name)), benchmark_parameters_(parameters), benchmark_result_(std::move(result)) {}

template <typename Parameters, typename BenchmarkResult, typename RepetitionResult, typename InvocationResult>
BenchmarkOutput<Parameters, BenchmarkResult, RepetitionResult, InvocationResult>&
BenchmarkOutput<Parameters, BenchmarkResult, RepetitionResult, InvocationResult>::WithBoolArgument(
    const Aws::String& name, bool value) {
  parameters_.WithBool(name, value);
  return *this;
}

template <typename Parameters, typename BenchmarkResult, typename RepetitionResult, typename InvocationResult>
BenchmarkOutput<Parameters, BenchmarkResult, RepetitionResult, InvocationResult>&
BenchmarkOutput<Parameters, BenchmarkResult, RepetitionResult, InvocationResult>::WithDoubleArgument(
    const Aws::String& name, double value) {
  parameters_.WithDouble(name, value);
  return *this;
}

template <typename Parameters, typename BenchmarkResult, typename RepetitionResult, typename InvocationResult>
BenchmarkOutput<Parameters, BenchmarkResult, RepetitionResult, InvocationResult>&
BenchmarkOutput<Parameters, BenchmarkResult, RepetitionResult, InvocationResult>::WithInt64Argument(
    const Aws::String& name, long long value) {
  parameters_.WithInt64(name, value);
  return *this;
}

template <typename Parameters, typename BenchmarkResult, typename RepetitionResult, typename InvocationResult>
BenchmarkOutput<Parameters, BenchmarkResult, RepetitionResult, InvocationResult>&
BenchmarkOutput<Parameters, BenchmarkResult, RepetitionResult, InvocationResult>::WithStringArgument(
    const Aws::String& name, const Aws::String& value) {
  parameters_.WithString(name, value);
  return *this;
}

template <typename Parameters, typename BenchmarkResult, typename RepetitionResult, typename InvocationResult>
BenchmarkOutput<Parameters, BenchmarkResult, RepetitionResult, InvocationResult>&
BenchmarkOutput<Parameters, BenchmarkResult, RepetitionResult, InvocationResult>::WithBoolMetric(
    const Aws::String& name, bool value) {
  metrics_.WithBool(name, value);
  return *this;
}

template <typename Parameters, typename BenchmarkResult, typename RepetitionResult, typename InvocationResult>
BenchmarkOutput<Parameters, BenchmarkResult, RepetitionResult, InvocationResult>&
BenchmarkOutput<Parameters, BenchmarkResult, RepetitionResult, InvocationResult>::WithDoubleMetric(
    const Aws::String& name, double value) {
  metrics_.WithDouble(name, value);
  return *this;
}

template <typename Parameters, typename BenchmarkResult, typename RepetitionResult, typename InvocationResult>
BenchmarkOutput<Parameters, BenchmarkResult, RepetitionResult, InvocationResult>&
BenchmarkOutput<Parameters, BenchmarkResult, RepetitionResult, InvocationResult>::WithInt64Metric(
    const Aws::String& name, long long value) {
  metrics_.WithInt64(name, value);
  return *this;
}

template <typename Parameters, typename BenchmarkResult, typename RepetitionResult, typename InvocationResult>
BenchmarkOutput<Parameters, BenchmarkResult, RepetitionResult, InvocationResult>&
BenchmarkOutput<Parameters, BenchmarkResult, RepetitionResult, InvocationResult>::WithStringMetric(
    const Aws::String& name, const Aws::String& value) {
  metrics_.WithString(name, value);
  return *this;
}

template <typename Parameters, typename BenchmarkResult, typename RepetitionResult, typename InvocationResult>
BenchmarkOutput<Parameters, BenchmarkResult, RepetitionResult, InvocationResult>&
BenchmarkOutput<Parameters, BenchmarkResult, RepetitionResult, InvocationResult>::WithBoolInvocationMetric(
    std::function<std::tuple<Aws::String, bool>(const InvocationResult&)> functor) {
  invocation_functors_.bool_functors.push_back(std::move(functor));
  return *this;
}

template <typename Parameters, typename BenchmarkResult, typename RepetitionResult, typename InvocationResult>
BenchmarkOutput<Parameters, BenchmarkResult, RepetitionResult, InvocationResult>&
BenchmarkOutput<Parameters, BenchmarkResult, RepetitionResult, InvocationResult>::WithDoubleInvocationMetric(
    std::function<std::tuple<Aws::String, double>(const InvocationResult&)> functor) {
  invocation_functors_.double_functors.push_back(std::move(functor));
  return *this;
}

template <typename Parameters, typename BenchmarkResult, typename RepetitionResult, typename InvocationResult>
BenchmarkOutput<Parameters, BenchmarkResult, RepetitionResult, InvocationResult>&
BenchmarkOutput<Parameters, BenchmarkResult, RepetitionResult, InvocationResult>::WithInt64InvocationMetric(
    std::function<std::tuple<Aws::String, long long>(const InvocationResult&)> functor) {
  invocation_functors_.int64_functors.push_back(std::move(functor));
  return *this;
}

template <typename Parameters, typename BenchmarkResult, typename RepetitionResult, typename InvocationResult>
BenchmarkOutput<Parameters, BenchmarkResult, RepetitionResult, InvocationResult>&
BenchmarkOutput<Parameters, BenchmarkResult, RepetitionResult, InvocationResult>::WithStringInvocationMetric(
    std::function<std::tuple<Aws::String, Aws::String>(const InvocationResult&)> functor) {
  invocation_functors_.string_functors.push_back(std::move(functor));
  return *this;
}

template <typename Parameters, typename BenchmarkResult, typename RepetitionResult, typename InvocationResult>
BenchmarkOutput<Parameters, BenchmarkResult, RepetitionResult, InvocationResult>&
BenchmarkOutput<Parameters, BenchmarkResult, RepetitionResult, InvocationResult>::WithObjectInvocationMetric(
    std::function<std::tuple<Aws::String, Aws::Utils::Json::JsonValue>(const InvocationResult&)> functor) {
  invocation_functors_.object_functors.push_back(std::move(functor));
  return *this;
}

template <typename Parameters, typename BenchmarkResult, typename RepetitionResult, typename InvocationResult>
BenchmarkOutput<Parameters, BenchmarkResult, RepetitionResult, InvocationResult>&
BenchmarkOutput<Parameters, BenchmarkResult, RepetitionResult, InvocationResult>::WithBoolRepetitionMetric(
    std::function<std::tuple<Aws::String, bool>(const RepetitionResult&)> functor) {
  repetition_functors_.bool_functors.push_back(std::move(functor));
  return *this;
}

template <typename Parameters, typename BenchmarkResult, typename RepetitionResult, typename InvocationResult>
BenchmarkOutput<Parameters, BenchmarkResult, RepetitionResult, InvocationResult>&
BenchmarkOutput<Parameters, BenchmarkResult, RepetitionResult, InvocationResult>::WithDoubleRepetitionMetric(
    std::function<std::tuple<Aws::String, double>(const RepetitionResult&)> functor) {
  repetition_functors_.double_functors.push_back(std::move(functor));
  return *this;
}

template <typename Parameters, typename BenchmarkResult, typename RepetitionResult, typename InvocationResult>
BenchmarkOutput<Parameters, BenchmarkResult, RepetitionResult, InvocationResult>&
BenchmarkOutput<Parameters, BenchmarkResult, RepetitionResult, InvocationResult>::WithInt64RepetitionMetric(
    std::function<std::tuple<Aws::String, long long>(const RepetitionResult&)> functor) {
  repetition_functors_.int64_functors.push_back(std::move(functor));
  return *this;
}

template <typename Parameters, typename BenchmarkResult, typename RepetitionResult, typename InvocationResult>
BenchmarkOutput<Parameters, BenchmarkResult, RepetitionResult, InvocationResult>&
BenchmarkOutput<Parameters, BenchmarkResult, RepetitionResult, InvocationResult>::WithStringRepetitionMetric(
    std::function<std::tuple<Aws::String, Aws::String>(const RepetitionResult&)> functor) {
  repetition_functors_.string_functors.push_back(std::move(functor));
  return *this;
}

template <typename Parameters, typename BenchmarkResult, typename RepetitionResult, typename InvocationResult>
BenchmarkOutput<Parameters, BenchmarkResult, RepetitionResult, InvocationResult>&
BenchmarkOutput<Parameters, BenchmarkResult, RepetitionResult, InvocationResult>::WithObjectRepetitionMetric(
    std::function<std::tuple<Aws::String, Aws::Utils::Json::JsonValue>(const RepetitionResult&)> functor) {
  repetition_functors_.object_functors.push_back(std::move(functor));
  return *this;
}

}  // namespace skyrise
