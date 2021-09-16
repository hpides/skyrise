#pragma once

#include <memory>
#include <tuple>

#include <aws/core/Aws.h>
#include <aws/core/utils/json/JsonSerializer.h>

#include "network_benchmark.hpp"
#include "utils/literal.hpp"

namespace skyrise {

class NetworkLatencyBenchmark : public NetworkBenchmark {
 public:
  NetworkLatencyBenchmark(std::shared_ptr<const BenchmarkHelper> helper,
                          std::shared_ptr<const CostCalculator> cost_calculator,
                          const std::vector<size_t>& function_instance_mb_sizes,
                          const std::vector<size_t>& object_byte_sizes, const std::vector<size_t>& batch_sizes,
                          const size_t repetition_count);

 private:
  Aws::Utils::Json::JsonValue GenerateResultOutput(const std::shared_ptr<LambdaBenchmarkResult>& benchmark_result,
                                                   const NetworkBenchmarkParameters& benchmark_parameters) override;
};

}  // namespace skyrise
