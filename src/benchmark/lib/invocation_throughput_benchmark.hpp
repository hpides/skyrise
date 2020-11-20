#pragma once

#include <memory>

#include <aws/core/Aws.h>
#include <aws/core/utils/json/JsonSerializer.h>

#include "benchmark.hpp"
#include "benchmark_config.hpp"
#include "benchmark_helper.hpp"
#include "benchmark_runner.hpp"

namespace skyrise {

// TODO(anyone): Recursive Lambda invocation
// TODO(anyone): VM (EC2) invocation
class InvocationThroughputBenchmark : public Benchmark {
 public:
  InvocationThroughputBenchmark(const std::vector<size_t>& function_sizes, const std::vector<size_t>& invocation_counts,
                                const std::vector<ExecuteMode>& execute_modes);

  Aws::Utils::Array<Aws::Utils::Json::JsonValue> Run(const std::shared_ptr<BenchmarkRunner>& benchmark_runner);

 private:
  static Aws::Utils::Json::JsonValue GenerateResultOutput(
      const std::shared_ptr<std::vector<BenchmarkItemResult>>& benchmark_result,
      const BenchmarkConfig& benchmark_config);

  std::vector<BenchmarkConfig> benchmark_configs_;

  const Aws::String kFunctionName = "skyriseFunctionMinimal";
};

}  // namespace skyrise
