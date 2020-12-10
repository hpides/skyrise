#pragma once

#include <aws/core/Aws.h>
#include <aws/core/utils/json/JsonSerializer.h>

#include "client/client_aws.hpp"
#include "network_benchmark.hpp"

namespace skyrise {

class NetworkThroughputParallelBenchmark : public NetworkBenchmark {
 public:
  NetworkThroughputParallelBenchmark(std::shared_ptr<BenchmarkHelper> helper,
                                     std::shared_ptr<CostCalculator> cost_calculator,
                                     const std::vector<size_t>& function_instance_counts, const size_t num_iterations);

 private:
  Aws::Utils::Json::JsonValue GenerateResultOutput(const std::shared_ptr<std::vector<BenchmarkItemResult>>& result,
                                                   const NetworkBenchmarkParameters& parameters) override;
  Aws::Utils::Json::JsonValue GenerateSubResultOutput(const Aws::String& benchmark_name, const size_t repetition,
                                                      const std::shared_ptr<std::vector<BenchmarkItemResult>>& result,
                                                      const NetworkBenchmarkParameters& parameters);
};

}  // namespace skyrise
