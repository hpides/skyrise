#pragma once

#include <memory>

#include <aws/core/Aws.h>
#include <aws/core/utils/json/JsonSerializer.h>

#include "lambda_benchmark.hpp"
#include "lambda_benchmark_config.hpp"
#include "lambda_benchmark_runner.hpp"
#include "utils/costs/cost_calculator.hpp"

namespace skyrise {

struct FunctionWarmUpContinuousBenchmarkParameters {
  size_t function_instance_mb_size;
  size_t invocation_count;
  size_t sleep_ms_duration;
  double provisioning_factor;
  size_t warm_up_min_interval;
  size_t repetition_count;
};

class FunctionWarmUpContinuousBenchmark : public LambdaBenchmark {
 public:
  FunctionWarmUpContinuousBenchmark(std::shared_ptr<const CostCalculator> cost_calculator,
                                    const std::vector<size_t>& function_instance_mb_sizes,
                                    const std::vector<size_t>& invocation_counts,
                                    const std::vector<size_t>& sleep_ms_durations,
                                    const std::vector<double>& provisioning_factors,
                                    const std::vector<size_t>& warm_up_min_intervals, const size_t repetition_count);
  const Aws::String& Name() const override;

 protected:
  Aws::Utils::Array<Aws::Utils::Json::JsonValue> OnRun(
      const std::shared_ptr<LambdaBenchmarkRunner>& benchmark_runner) override;

 private:
  Aws::Utils::Json::JsonValue GenerateResultOutput(
      const std::shared_ptr<LambdaBenchmarkResult>& benchmark_result,
      const FunctionWarmUpContinuousBenchmarkParameters& benchmark_parameters) const;

  std::vector<std::pair<FunctionWarmUpContinuousBenchmarkParameters, std::shared_ptr<LambdaBenchmarkConfig>>>
      benchmark_configs_;

  inline static const Aws::String kFunctionName{"skyriseFunctionSimple"};
};

}  // namespace skyrise
