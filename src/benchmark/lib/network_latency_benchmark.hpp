#pragma once

#include <memory>
#include <tuple>

#include <aws/core/Aws.h>
#include <aws/core/utils/json/JsonSerializer.h>

#include "client/client_aws.hpp"
#include "network_benchmark.hpp"
#include "utils/literal.hpp"

namespace skyrise {

class NetworkLatencyBenchmark : public NetworkBenchmark {
 public:
  NetworkLatencyBenchmark(std::shared_ptr<BenchmarkHelper> helper, std::shared_ptr<CostCalculator> cost_calculator,
                          const std::vector<size_t>& function_instance_mb_sizes,
                          const std::vector<size_t>& object_byte_sizes_read,
                          const std::vector<size_t>& object_byte_sizes_write, const size_t repetition_count);

 private:
  Aws::Utils::Json::JsonValue GenerateResultOutput(const std::shared_ptr<std::vector<BenchmarkItemResult>>& result,
                                                   const NetworkBenchmarkParameters& parameters) override;

  const size_t kObjectBytesSize = 1_KB;
};

}  // namespace skyrise
