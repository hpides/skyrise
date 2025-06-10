#pragma once

#include "benchmark_output.hpp"
#include "lambda_benchmark.hpp"
#include "lambda_benchmark_result.hpp"

namespace skyrise {

class LambdaBenchmarkOutput : public BenchmarkOutput<LambdaBenchmarkParameters, LambdaBenchmarkResult,
                                                     LambdaBenchmarkRepetitionResult, LambdaInvocationResult> {
 public:
  LambdaBenchmarkOutput(Aws::String name, const LambdaBenchmarkParameters& parameters,
                        std::shared_ptr<LambdaBenchmarkResult> result);

  Aws::Utils::Json::JsonValue Build() const override;

  double GetLambdaRuntimeCosts() const;
};

}  // namespace skyrise
