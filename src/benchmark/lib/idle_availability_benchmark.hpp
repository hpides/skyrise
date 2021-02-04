#pragma once

#include <memory>

#include <aws/core/Aws.h>
#include <aws/core/utils/json/JsonSerializer.h>

#include "benchmark.hpp"
#include "benchmark_config.hpp"
#include "benchmark_helper.hpp"
#include "benchmark_runner.hpp"

namespace skyrise {

// TODO(maltenbergert): Consolidate this benchmark with other HostBenchmarks
struct IdleAvailabilityBenchmarkParameters {
  size_t function_instance_mb_size;
  size_t invocation_count;
  size_t sleep_min_duration;
  size_t repetition_count;
};

class IdleAvailabilityBenchmark : public Benchmark {
 public:
  IdleAvailabilityBenchmark(const std::vector<size_t>& function_instance_mb_sizes,
                            const std::vector<size_t>& invocation_counts,
                            const std::vector<size_t>& sleep_min_durations, const size_t repetition_count);

  Aws::Utils::Array<Aws::Utils::Json::JsonValue> Run(const std::shared_ptr<BenchmarkRunner>& benchmark_runner);

 private:
  static Aws::Utils::Json::JsonValue GenerateResultOutput(
      const std::shared_ptr<BenchmarkResult>& benchmark_result,
      const IdleAvailabilityBenchmarkParameters& benchmark_parameters);

  std::vector<std::pair<IdleAvailabilityBenchmarkParameters, BenchmarkConfig>> benchmark_configs_;

  const Aws::String kFunctionName = "skyriseFunctionHostId";
};

}  // namespace skyrise
