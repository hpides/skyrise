#pragma once

#include <memory>

#include <aws/core/Aws.h>
#include <aws/core/utils/json/JsonSerializer.h>

#include "benchmark_helper.hpp"
#include "lambda_benchmark.hpp"
#include "lambda_benchmark_config.hpp"
#include "lambda_benchmark_runner.hpp"
#include "utils/costs/cost_calculator.hpp"

namespace skyrise {

// TODO(maltenbergert): Consolidate this benchmark with other HostBenchmarks
struct FunctionColocationBenchmarkParameters {
  size_t function_instance_mb_size;
  size_t invocation_count;
  size_t sleep_min_duration;
  size_t repetition_count;
};

class FunctionColocationBenchmark : public LambdaBenchmark {
 public:
  FunctionColocationBenchmark(std::shared_ptr<const CostCalculator> cost_calculator,
                              const std::vector<size_t>& function_instance_mb_sizes,
                              const std::vector<size_t>& invocation_counts,
                              const std::vector<size_t>& sleep_min_durations, const size_t repetition_count);

 protected:
  Aws::Utils::Array<Aws::Utils::Json::JsonValue> OnRun(
      const std::shared_ptr<LambdaBenchmarkRunner>& benchmark_runner) override;

 private:
  Aws::Utils::Json::JsonValue GenerateResultOutput(
      const std::shared_ptr<LambdaBenchmarkResult>& benchmark_result,
      const FunctionColocationBenchmarkParameters& benchmark_parameters) const;

  std::vector<std::pair<FunctionColocationBenchmarkParameters, std::shared_ptr<LambdaBenchmarkConfig>>>
      benchmark_configs_;

  const Aws::String kFunctionName{"skyriseFunctionHostId"};
};

}  // namespace skyrise
