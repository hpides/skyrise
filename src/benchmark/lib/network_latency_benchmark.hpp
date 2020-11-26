#pragma once

#include <memory>
#include <tuple>

#include <aws/core/Aws.h>
#include <aws/core/utils/json/JsonSerializer.h>

#include "client/client_aws.hpp"
#include "network_benchmark.hpp"

namespace skyrise {

class NetworkLatencyBenchmark : public NetworkBenchmark {
 public:
  NetworkLatencyBenchmark(std::shared_ptr<BenchmarkHelper> helper, std::shared_ptr<CostCalculator> cost_calculator,
                          const size_t num_iterations, const ExecuteMode execute_mode,
                          const std::vector<size_t>& function_instance_mb_sizes);

 private:
  Aws::Utils::Json::JsonValue GenerateResultOutput(const std::shared_ptr<std::vector<BenchmarkItemResult>>& result,
                                                   const NetworkBenchmarkParameters& parameters) override;
};

}  // namespace skyrise
