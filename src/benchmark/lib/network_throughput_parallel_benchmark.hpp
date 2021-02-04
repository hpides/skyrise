#pragma once

#include <aws/core/Aws.h>
#include <aws/core/utils/json/JsonSerializer.h>

#include "network_benchmark.hpp"

namespace skyrise {

class NetworkThroughputParallelBenchmark : public NetworkBenchmark {
 public:
  NetworkThroughputParallelBenchmark(std::shared_ptr<BenchmarkHelper> helper,
                                     std::shared_ptr<CostCalculator> cost_calculator,
                                     const std::vector<size_t>& invocation_counts, const size_t batch_size,
                                     const size_t repetition_count);

 private:
  Aws::Utils::Json::JsonValue GenerateResultOutput(const std::shared_ptr<BenchmarkResult>& benchmark_result,
                                                   const NetworkBenchmarkParameters& benchmark_parameters) override;
};

}  // namespace skyrise
