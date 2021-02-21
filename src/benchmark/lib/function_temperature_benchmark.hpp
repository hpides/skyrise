#pragma once

#include <memory>

#include <aws/core/Aws.h>
#include <aws/core/utils/json/JsonSerializer.h>

#include "benchmark.hpp"
#include "benchmark_config.hpp"
#include "benchmark_runner.hpp"
#include "utils/costs/cost_calculator.hpp"

namespace skyrise {

struct FunctionTemperatureBenchmarkParameters {
  size_t function_instance_mb_size;
  size_t invocation_count;
  size_t repetition_count;
  std::shared_ptr<WarmUpStrategy> warm_up_strategy;
};

class FunctionTemperatureBenchmark : public Benchmark {
 public:
  FunctionTemperatureBenchmark(std::shared_ptr<CostCalculator> cost_calculator,
                               const std::vector<size_t>& function_instance_mb_sizes,
                               const std::vector<size_t>& invocation_counts, const size_t repetition_count);

  Aws::Utils::Array<Aws::Utils::Json::JsonValue> Run(const std::shared_ptr<BenchmarkRunner>& benchmark_runner);

 private:
  Aws::Utils::Json::JsonValue GenerateResultOutput(
      const std::shared_ptr<BenchmarkResult>& benchmark_result,
      const FunctionTemperatureBenchmarkParameters& benchmark_parameters) const;

  std::vector<std::pair<FunctionTemperatureBenchmarkParameters, BenchmarkConfig>> benchmark_configs_;

  const Aws::String kFunctionName = "skyriseFunctionSimple";
  // TODO(anyone): Eliminate magic number once we understand the parallel running lambda functions better
  const size_t kSleepMs = 7000;
};

}  // namespace skyrise
