#pragma once

#include <aws/core/Aws.h>
#include <aws/core/utils/json/JsonSerializer.h>

#include "network_benchmark.hpp"

namespace skyrise {

class NetworkThroughputParallelBenchmark : public NetworkBenchmark {
 public:
  NetworkThroughputParallelBenchmark(std::shared_ptr<const BenchmarkHelper> helper,
                                     std::shared_ptr<const CostCalculator> cost_calculator,
                                     const std::vector<size_t>& function_instance_mb_sizes,
                                     const std::vector<size_t>& object_byte_sizes,
                                     const std::vector<size_t>& batch_sizes, const std::vector<size_t>& thread_counts,
                                     const std::vector<size_t>& invocation_counts,
                                     const std::vector<size_t>& bucket_counts, const bool enable_reads,
                                     const size_t repetition_count);

 private:
  Aws::Utils::Json::JsonValue GenerateResultOutput(const std::shared_ptr<BenchmarkResult>& benchmark_result,
                                                   const NetworkBenchmarkParameters& benchmark_parameters) override;
};

}  // namespace skyrise
