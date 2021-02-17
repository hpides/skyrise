#pragma once

#include <memory>

#include <aws/core/Aws.h>
#include <aws/core/utils/json/JsonSerializer.h>

#include "benchmark.hpp"
#include "benchmark_config.hpp"
#include "benchmark_helper.hpp"
#include "benchmark_runner.hpp"
#include "utils/costs/cost_calculator.hpp"

namespace skyrise {

struct InvocationThroughputBenchmarkParameters {
  size_t function_instance_mb_size;
  size_t invocation_count;
  size_t function_payload_byte_size;
  size_t repetition_count;
  UseEventQueue use_event_queue;
};

class InvocationThroughputBenchmark : public Benchmark {
 public:
  InvocationThroughputBenchmark(std::shared_ptr<CostCalculator> cost_calculator,
                                const std::vector<size_t>& function_instance_mb_sizes,
                                const std::vector<size_t>& invocation_counts,
                                const std::vector<size_t>& function_payload_byte_sizes, const size_t repetition_count);

  Aws::Utils::Array<Aws::Utils::Json::JsonValue> Run(const std::shared_ptr<BenchmarkRunner>& benchmark_runner);

 private:
  Aws::Utils::Json::JsonValue GenerateResultOutput(
      const std::shared_ptr<BenchmarkResult>& benchmark_result,
      const InvocationThroughputBenchmarkParameters& benchmark_parameters) const;

  std::vector<std::pair<InvocationThroughputBenchmarkParameters, BenchmarkConfig>> benchmark_configs_;

  const Aws::String kFunctionName = "skyriseFunctionSimple";
};

}  // namespace skyrise
