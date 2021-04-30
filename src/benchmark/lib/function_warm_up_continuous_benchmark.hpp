#pragma once

#include <memory>

#include <aws/core/Aws.h>
#include <aws/core/utils/json/JsonSerializer.h>

#include "benchmark.hpp"
#include "benchmark_config.hpp"
#include "benchmark_runner.hpp"
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

class FunctionWarmUpContinuousBenchmark : public Benchmark {
 public:
  FunctionWarmUpContinuousBenchmark(std::shared_ptr<CostCalculator> cost_calculator,
                                    const std::vector<size_t>& function_instance_mb_sizes,
                                    const std::vector<size_t>& invocation_counts,
                                    const std::vector<size_t>& sleep_ms_durations,
                                    const std::vector<double>& provisioning_factors,
                                    const std::vector<size_t>& warm_up_min_intervals, const size_t repetition_count);

  Aws::Utils::Array<Aws::Utils::Json::JsonValue> Run(const std::shared_ptr<BenchmarkRunner>& benchmark_runner);

 private:
  Aws::Utils::Json::JsonValue GenerateResultOutput(
      const std::shared_ptr<BenchmarkResult>& benchmark_result,
      const FunctionWarmUpContinuousBenchmarkParameters& benchmark_parameters) const;

  std::vector<std::pair<FunctionWarmUpContinuousBenchmarkParameters, BenchmarkConfig>> benchmark_configs_;

  inline static const Aws::String kFunctionName{"skyriseFunctionSimple"};
};

}  // namespace skyrise
