#pragma once

#include <vector>

#include <aws/core/Aws.h>

#include "lambda_benchmark_result.hpp"

namespace skyrise {

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

  std::vector<std::function<std::tuple<Aws::String, bool>(const LambdaInvokeResult&)>> bool_invocation_functors_;
  std::vector<std::function<std::tuple<Aws::String, long long>(const LambdaInvokeResult&)>> int64_invocation_functors_;
  std::vector<std::function<std::tuple<Aws::String, double>(const LambdaInvokeResult&)>> double_invocation_functors_;
  std::vector<std::function<std::tuple<Aws::String, Aws::String>(const LambdaInvokeResult&)>>
      string_invocation_functors_;
  std::vector<std::function<std::tuple<Aws::String, Aws::Utils::Json::JsonValue>(const LambdaInvokeResult&)>>
      object_invocation_functors_;
};

}  // namespace skyrise
