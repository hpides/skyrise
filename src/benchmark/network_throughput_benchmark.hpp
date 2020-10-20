#pragma once

#include <aws/core/Aws.h>
#include <aws/core/utils/json/JsonSerializer.h>

#include "benchmark_config.hpp"
#include "benchmark_runner.hpp"
#include "client/client_aws.hpp"
#include "utils/benchmark_helper.hpp"
#include "utils/costs/cost_calculator.hpp"

namespace skyrise {

// TODO(David): Add an AbstractNetworkBenchmark class

struct NetworkThroughputBenchmarkResult {
  size_t function_instance_mb_size_;
  size_t object_mb_size_;
  size_t thread_count_;
  std::shared_ptr<std::vector<BenchmarkItemResult>> results_;
};

class NetworkThroughputBenchmark {
 public:
  NetworkThroughputBenchmark(std::shared_ptr<BenchmarkHelper> helper, std::shared_ptr<CostCalculator> cost_calculator,
                             const std::vector<size_t>& function_instance_mb_sizes,
                             const std::vector<size_t>& object_mb_sizes, const std::vector<size_t>& thread_counts,
                             ExecuteMode execute_mode, size_t num_iterations);

  Aws::Utils::Array<Aws::Utils::Json::JsonValue> Run(const std::shared_ptr<BenchmarkRunner>& benchmark_runner);

 private:
  void Setup();
  void Teardown();

  long double CalculateBenchmarkCost(const std::shared_ptr<std::vector<BenchmarkItemResult>>& result,
                                     const size_t function_instance_mb_size);

  std::vector<std::shared_ptr<Aws::IOStream>> GeneratePayloads(const size_t function_instance_mb_size,
                                                               const size_t object_mb_size, const size_t thread_count);
  Aws::Utils::Json::JsonValue GenerateResultOutput(const NetworkThroughputBenchmarkResult& result,
                                                   const size_t num_results);

  long double ExtractFunctionCost(const BenchmarkItemResult& result, const size_t function_instance_mb_size);

  const std::shared_ptr<BenchmarkHelper> helper_;
  const std::shared_ptr<CostCalculator> cost_calculator_;

  const std::vector<size_t> function_instance_mb_sizes_;
  const std::vector<size_t> object_mb_sizes_;
  const std::vector<size_t> thread_counts_;
  const ExecuteMode execute_mode_;
  const size_t num_iterations_;

  std::vector<BenchmarkConfig> benchmark_configs_;

  long double cost_overhead_;

  const Aws::String kFunctionName = "skyriseFunctionReadWriteS3";
  const Aws::String kJsonGetObjectDurationKey = "get_object_duration_ms";
  const Aws::String kJsonPutObjectDurationKey = "put_object_duration_ms";
  const Aws::String kObjectKey = "s3-object";
  const Aws::String kReadBucket = "network-throughput-benchmark-read";
  const Aws::String kWriteBucket = "network-throughput-benchmark-write";
};

}  // namespace skyrise
