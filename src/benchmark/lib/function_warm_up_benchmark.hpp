#pragma once

#include <memory>

#include <aws/core/Aws.h>
#include <aws/core/utils/json/JsonSerializer.h>

#include "benchmark.hpp"
#include "benchmark_config.hpp"
#include "benchmark_runner.hpp"
#include "utils/costs/cost_calculator.hpp"

namespace skyrise {

struct FunctionWarmUpBenchmarkParameters {
  size_t function_instance_mb_size;
  size_t invocation_count;
  size_t repetition_count;
  size_t sleep_ms_duration;
  double provisioning_factor;
  std::string warm_up_strategy;
};

class FunctionWarmUpBenchmark : public Benchmark {
 public:
  FunctionWarmUpBenchmark(std::shared_ptr<CostCalculator> cost_calculator,
                          const std::vector<size_t>& function_instance_mb_sizes,
                          const std::vector<size_t>& invocation_counts, const std::vector<size_t>& sleep_ms_durations,
                          const std::vector<double>& provisioning_factors, const size_t repetition_count);

  Aws::Utils::Array<Aws::Utils::Json::JsonValue> Run(const std::shared_ptr<BenchmarkRunner>& benchmark_runner);

 private:
  Aws::Utils::Json::JsonValue GenerateResultOutput(const std::shared_ptr<BenchmarkResult>& benchmark_result,
                                                   const FunctionWarmUpBenchmarkParameters& benchmark_parameters) const;

  std::vector<std::pair<FunctionWarmUpBenchmarkParameters, BenchmarkConfig>> benchmark_configs_;

  const Aws::String kFunctionName{"skyriseFunctionSimple"};

  // TODO(anyone): Eliminate magic number once we understand the parallel running lambda functions better
  static constexpr size_t kFunctionSleepMs = 7000;
  static constexpr size_t kRepetitionSleepMin = 5;
};

}  // namespace skyrise
