#pragma once

#include <vector>

#include <aws/core/Aws.h>

namespace skyrise {

template <class T>
struct MetricFunctors {
 public:
  Aws::Utils::Json::JsonValue& AppendMetrics(T result, Aws::Utils::Json::JsonValue& output) const;

  std::vector<std::function<std::tuple<Aws::String, bool>(const T&)>> bool_functors;
  std::vector<std::function<std::tuple<Aws::String, double>(const T&)>> double_functors;
  std::vector<std::function<std::tuple<Aws::String, long long>(const T&)>> int64_functors;
  std::vector<std::function<std::tuple<Aws::String, Aws::String>(const T&)>> string_functors;
  std::vector<std::function<std::tuple<Aws::String, Aws::Utils::Json::JsonValue>(const T&)>> object_functors;
};

template <typename Parameters, typename BenchmarkResult, typename RepetitionResult, typename InvocationResult>
class BenchmarkOutput {
 public:
  BenchmarkOutput(Aws::String name, const Parameters& parameters, std::shared_ptr<BenchmarkResult> result);

  virtual ~BenchmarkOutput() = default;

  BenchmarkOutput& WithBoolArgument(const Aws::String& name, bool value);
  BenchmarkOutput& WithDoubleArgument(const Aws::String& name, double value);
  BenchmarkOutput& WithInt64Argument(const Aws::String& name, long long value);
  BenchmarkOutput& WithStringArgument(const Aws::String& name, const Aws::String& value);

  BenchmarkOutput& WithBoolMetric(const Aws::String& name, bool value);
  BenchmarkOutput& WithDoubleMetric(const Aws::String& name, double value);
  BenchmarkOutput& WithInt64Metric(const Aws::String& name, long long value);
  BenchmarkOutput& WithStringMetric(const Aws::String& name, const Aws::String& value);

  BenchmarkOutput& WithBoolRepetitionMetric(
      std::function<std::tuple<Aws::String, bool>(const RepetitionResult&)> functor);
  BenchmarkOutput& WithDoubleRepetitionMetric(
      std::function<std::tuple<Aws::String, double>(const RepetitionResult&)> functor);
  BenchmarkOutput& WithInt64RepetitionMetric(
      std::function<std::tuple<Aws::String, long long>(const RepetitionResult&)> functor);
  BenchmarkOutput& WithStringRepetitionMetric(
      std::function<std::tuple<Aws::String, Aws::String>(const RepetitionResult&)> functor);
  BenchmarkOutput& WithObjectRepetitionMetric(
      std::function<std::tuple<Aws::String, Aws::Utils::Json::JsonValue>(const RepetitionResult&)> functor);

  BenchmarkOutput& WithBoolInvocationMetric(
      std::function<std::tuple<Aws::String, bool>(const InvocationResult&)> functor);
  BenchmarkOutput& WithDoubleInvocationMetric(
      std::function<std::tuple<Aws::String, double>(const InvocationResult&)> functor);
  BenchmarkOutput& WithInt64InvocationMetric(
      std::function<std::tuple<Aws::String, long long>(const InvocationResult&)> functor);
  BenchmarkOutput& WithStringInvocationMetric(
      std::function<std::tuple<Aws::String, Aws::String>(const InvocationResult&)> functor);
  BenchmarkOutput& WithObjectInvocationMetric(
      std::function<std::tuple<Aws::String, Aws::Utils::Json::JsonValue>(const InvocationResult&)> functor);

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
