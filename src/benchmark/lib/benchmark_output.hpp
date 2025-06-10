#pragma once

#include <vector>

#include <aws/core/Aws.h>

namespace skyrise {

template <class T>
struct MetricFunctors {
 public:
  Aws::Utils::Json::JsonValue& AppendMetrics(const T& result, Aws::Utils::Json::JsonValue& output) const {
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

  std::vector<std::function<std::tuple<Aws::String, bool>(const T&)>> bool_functors;
  std::vector<std::function<std::tuple<Aws::String, double>(const T&)>> double_functors;
  std::vector<std::function<std::tuple<Aws::String, long long>(const T&)>> int64_functors;
  std::vector<std::function<std::tuple<Aws::String, Aws::String>(const T&)>> string_functors;
  std::vector<std::function<std::tuple<Aws::String, Aws::Utils::Json::JsonValue>(const T&)>> object_functors;
};

template <typename Parameters, typename BenchmarkResult, typename RepetitionResult, typename InvocationResult>
class BenchmarkOutput {
 public:
  // BenchmarkOutput(Aws::String name, const Parameters& parameters, std::shared_ptr<BenchmarkResult> result);
  BenchmarkOutput(Aws::String name, const Parameters& parameters, std::shared_ptr<BenchmarkResult> result)
      : benchmark_name_(std::move(name)), benchmark_parameters_(parameters), benchmark_result_(std::move(result)) {}

  virtual ~BenchmarkOutput() = default;

  BenchmarkOutput& WithBoolArgument(const Aws::String& name, bool value) {
    parameters_.WithBool(name, value);
    return *this;
  }

  BenchmarkOutput& WithDoubleArgument(const Aws::String& name, double value) {
    parameters_.WithDouble(name, value);
    return *this;
  }

  BenchmarkOutput& WithInt64Argument(const Aws::String& name, long long value) {
    parameters_.WithInt64(name, value);
    return *this;
  }

  BenchmarkOutput& WithStringArgument(const Aws::String& name, const Aws::String& value) {
    parameters_.WithString(name, value);
    return *this;
  }

  BenchmarkOutput& WithBoolMetric(const Aws::String& name, bool value) {
    metrics_.WithBool(name, value);
    return *this;
  }

  BenchmarkOutput& WithDoubleMetric(const Aws::String& name, double value) {
    metrics_.WithDouble(name, value);
    return *this;
  }

  BenchmarkOutput& WithInt64Metric(const Aws::String& name, long long value) {
    metrics_.WithInt64(name, value);
    return *this;
  }

  BenchmarkOutput& WithStringMetric(const Aws::String& name, const Aws::String& value) {
    metrics_.WithString(name, value);
    return *this;
  }

  BenchmarkOutput& WithBoolInvocationMetric(
      std::function<std::tuple<Aws::String, bool>(const InvocationResult&)> functor) {
    invocation_functors_.bool_functors.push_back(std::move(functor));
    return *this;
  }

  BenchmarkOutput& WithDoubleInvocationMetric(
      std::function<std::tuple<Aws::String, double>(const InvocationResult&)> functor) {
    invocation_functors_.double_functors.push_back(std::move(functor));
    return *this;
  }

  BenchmarkOutput& WithInt64InvocationMetric(
      std::function<std::tuple<Aws::String, long long>(const InvocationResult&)> functor) {
    invocation_functors_.int64_functors.push_back(std::move(functor));
    return *this;
  }

  BenchmarkOutput& WithStringInvocationMetric(
      std::function<std::tuple<Aws::String, Aws::String>(const InvocationResult&)> functor) {
    invocation_functors_.string_functors.push_back(std::move(functor));
    return *this;
  }

  BenchmarkOutput& WithObjectInvocationMetric(
      std::function<std::tuple<Aws::String, Aws::Utils::Json::JsonValue>(const InvocationResult&)> functor) {
    invocation_functors_.object_functors.push_back(std::move(functor));
    return *this;
  }

  BenchmarkOutput& WithBoolRepetitionMetric(
      std::function<std::tuple<Aws::String, bool>(const RepetitionResult&)> functor) {
    repetition_functors_.bool_functors.push_back(std::move(functor));
    return *this;
  }

  BenchmarkOutput& WithDoubleRepetitionMetric(
      std::function<std::tuple<Aws::String, double>(const RepetitionResult&)> functor) {
    repetition_functors_.double_functors.push_back(std::move(functor));
    return *this;
  }

  BenchmarkOutput& WithInt64RepetitionMetric(
      std::function<std::tuple<Aws::String, long long>(const RepetitionResult&)> functor) {
    repetition_functors_.int64_functors.push_back(std::move(functor));
    return *this;
  }

  BenchmarkOutput& WithStringRepetitionMetric(
      std::function<std::tuple<Aws::String, Aws::String>(const RepetitionResult&)> functor) {
    repetition_functors_.string_functors.push_back(std::move(functor));
    return *this;
  }

  BenchmarkOutput& WithObjectRepetitionMetric(
      std::function<std::tuple<Aws::String, Aws::Utils::Json::JsonValue>(const RepetitionResult&)> functor) {
    repetition_functors_.object_functors.push_back(std::move(functor));
    return *this;
  }

  virtual Aws::Utils::Json::JsonValue Build() const = 0;

 protected:
  const Aws::String benchmark_name_;
  const Parameters& benchmark_parameters_;
  const std::shared_ptr<BenchmarkResult> benchmark_result_;

  Aws::Utils::Json::JsonValue parameters_;
  Aws::Utils::Json::JsonValue metrics_;

  MetricFunctors<InvocationResult> invocation_functors_;
  MetricFunctors<RepetitionResult> repetition_functors_;
};

}  // namespace skyrise
