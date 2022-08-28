#pragma once

#include <vector>

#include <aws/core/Aws.h>

#include "lambda_benchmark_result.hpp"

namespace skyrise {

template <class T>
struct MetricFunctors {
 public:
  Aws::Utils::Json::JsonValue& AppendMetrics(T result, Aws::Utils::Json::JsonValue& output) const;

  std::vector<std::function<std::tuple<Aws::String, bool>(const T&)>> bool_functors;
  std::vector<std::function<std::tuple<Aws::String, long long>(const T&)>> int64_functors;
  std::vector<std::function<std::tuple<Aws::String, double>(const T&)>> double_functors;
  std::vector<std::function<std::tuple<Aws::String, Aws::String>(const T&)>> string_functors;
  std::vector<std::function<std::tuple<Aws::String, Aws::Utils::Json::JsonValue>(const T&)>> object_functors;
};

class LambdaBenchmarkOutput {
 public:
  LambdaBenchmarkOutput(Aws::String benchmark_name, std::shared_ptr<LambdaBenchmarkResult> benchmark_result);

  LambdaBenchmarkOutput& WithBoolArgument(const Aws::String& name, bool value);
  LambdaBenchmarkOutput& WithInt64Argument(const Aws::String& name, long long value);
  LambdaBenchmarkOutput& WithDoubleArgument(const Aws::String& name, double value);
  LambdaBenchmarkOutput& WithStringArgument(const Aws::String& name, const Aws::String& value);

  LambdaBenchmarkOutput& WithBoolMetric(const Aws::String& name, bool value);
  LambdaBenchmarkOutput& WithInt64Metric(const Aws::String& name, long long value);
  LambdaBenchmarkOutput& WithDoubleMetric(const Aws::String& name, double value);
  LambdaBenchmarkOutput& WithStringMetric(const Aws::String& name, const Aws::String& value);

  LambdaBenchmarkOutput& WithBoolRepetitionMetric(
      std::function<std::tuple<Aws::String, bool>(const LambdaBenchmarkRepetition&)> functor);
  LambdaBenchmarkOutput& WithInt64RepetitionMetric(
      std::function<std::tuple<Aws::String, long long>(const LambdaBenchmarkRepetition&)> functor);
  LambdaBenchmarkOutput& WithDoubleRepetitionMetric(
      std::function<std::tuple<Aws::String, double>(const LambdaBenchmarkRepetition&)> functor);
  LambdaBenchmarkOutput& WithStringRepetitionMetric(
      std::function<std::tuple<Aws::String, Aws::String>(const LambdaBenchmarkRepetition&)> functor);
  LambdaBenchmarkOutput& WithObjectRepetitionMetric(
      std::function<std::tuple<Aws::String, Aws::Utils::Json::JsonValue>(const LambdaBenchmarkRepetition&)> functor);

  LambdaBenchmarkOutput& WithBoolInvocationMetric(
      std::function<std::tuple<Aws::String, bool>(const LambdaInvokeResult&)> functor);
  LambdaBenchmarkOutput& WithInt64InvocationMetric(
      std::function<std::tuple<Aws::String, long long>(const LambdaInvokeResult&)> functor);
  LambdaBenchmarkOutput& WithDoubleInvocationMetric(
      std::function<std::tuple<Aws::String, double>(const LambdaInvokeResult&)> functor);
  LambdaBenchmarkOutput& WithStringInvocationMetric(
      std::function<std::tuple<Aws::String, Aws::String>(const LambdaInvokeResult&)> functor);
  LambdaBenchmarkOutput& WithObjectInvocationMetric(
      std::function<std::tuple<Aws::String, Aws::Utils::Json::JsonValue>(const LambdaInvokeResult&)> functor);

  Aws::Utils::Json::JsonValue Build() const;

 private:
  const Aws::String benchmark_name_;
  const std::shared_ptr<LambdaBenchmarkResult> benchmark_result_;

  Aws::Utils::Json::JsonValue arguments_;
  Aws::Utils::Json::JsonValue metrics_;

  MetricFunctors<LambdaBenchmarkRepetition> repetition_functors_;
  MetricFunctors<LambdaInvokeResult> invocation_functors_;
};

}  // namespace skyrise
