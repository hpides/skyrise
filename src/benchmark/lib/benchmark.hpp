#pragma once

#include <aws/core/Aws.h>

#include "benchmark_result.hpp"
#include "benchmark_runner.hpp"
#include "utils/costs/cost_calculator.hpp"

namespace skyrise {

class Benchmark {
 public:
  Benchmark(std::shared_ptr<CostCalculator> cost_calculator);

  Benchmark(const Benchmark&) = delete;
  Benchmark& operator=(const Benchmark&) = delete;

  virtual ~Benchmark() = default;

  virtual Aws::Utils::Array<Aws::Utils::Json::JsonValue> Run(
      const std::shared_ptr<BenchmarkRunner>& benchmark_runner) = 0;

 protected:
  long double CalculateOverallFunctionCost(const std::shared_ptr<BenchmarkResult>& result,
                                           const size_t function_instance_mb_size) const;
  long double ExtractFunctionCost(const InvocationResult& result, const size_t function_instance_mb_size) const;

  // TODO(maltenbergert): Move parts of BenchmarkHelper here

  std::shared_ptr<CostCalculator> cost_calculator_;
};

}  // namespace skyrise
