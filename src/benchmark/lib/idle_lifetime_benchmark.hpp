#pragma once

#include <memory>

#include <aws/core/Aws.h>
#include <aws/core/utils/json/JsonSerializer.h>

#include "benchmark.hpp"
#include "benchmark_config.hpp"
#include "benchmark_helper.hpp"
#include "benchmark_runner.hpp"

namespace skyrise {

class IdleLifetimeBenchmark : public Benchmark {
 public:
  IdleLifetimeBenchmark(const std::vector<size_t>& function_instance_mb_sizes,
                        const std::vector<size_t>& invocation_counts, const std::vector<size_t>& sleep_min_durations);

  Aws::Utils::Array<Aws::Utils::Json::JsonValue> Run(const std::shared_ptr<BenchmarkRunner>& benchmark_runner);

 private:
  Aws::Utils::Json::JsonValue GenerateResultOutput(
      const std::shared_ptr<std::vector<BenchmarkItemResult>>& benchmark_result,
      const BenchmarkConfig& benchmark_config) const;

  std::vector<BenchmarkConfig> benchmark_configs_;

  std::vector<size_t> sleep_min_durations_;

  const Aws::String kFunctionName = "skyriseFunctionHostId";
  const ExecuteMode kExecuteMode = ExecuteMode::kColdParallel;
};

}  // namespace skyrise
