#pragma once

#include <memory>

#include <aws/core/Aws.h>
#include <aws/core/utils/json/JsonSerializer.h>

#include "benchmark_config.hpp"
#include "benchmark_runner.hpp"
#include "client/client_aws.hpp"
#include "utils/benchmark_helper.hpp"
#include "utils/costs/cost_calculator.hpp"
#include "utils/literal.hpp"

namespace skyrise {

// TODO(d-justen): Add an AbstractNetworkBenchmark class

class NetworkLatencyBenchmark {
 public:
  NetworkLatencyBenchmark(std::shared_ptr<BenchmarkHelper> helper, std::shared_ptr<CostCalculator> cost_calculator,
                          const std::vector<size_t>& function_instance_sizes, const ExecuteMode execute_mode,
                          const size_t num_iterations);
  Aws::Utils::Array<Aws::Utils::Json::JsonValue> Run(const std::shared_ptr<BenchmarkRunner>& benchmark_runner);

 private:
  void Setup();
  void Teardown();

  long double CalculateBenchmarkCost(const std::shared_ptr<std::vector<BenchmarkItemResult>>& result,
                                     const size_t function_instance_size);

  std::vector<std::shared_ptr<Aws::IOStream>> GeneratePayloads(const size_t function_instance_size);
  Aws::Utils::Json::JsonValue GenerateResultOutput(const std::shared_ptr<std::vector<BenchmarkItemResult>>& result,
                                                   const size_t function_instance_size);

  long double ExtractFunctionCost(const BenchmarkItemResult& result, const size_t function_instance_size);

  const std::shared_ptr<BenchmarkHelper> helper_;
  const std::shared_ptr<CostCalculator> cost_calculator_;

  const std::vector<size_t> function_instance_sizes_;
  const ExecuteMode execute_mode_;
  const size_t num_iterations_;

  long double cost_overhead_;

  std::vector<BenchmarkConfig> benchmark_configs_;

  const Aws::String kFunctionName = "skyriseFunctionReadWriteS3";
  const Aws::String kJsonGetObjectDurationKey = "get_object_duration_ms";
  const Aws::String kJsonPutObjectDurationKey = "put_object_duration_ms";
  const Aws::String kObjectKey = "s3-object-1kb";
  const Aws::String kReadBucket = "network-latency-benchmark-read";
  const Aws::String kWriteBucket = "network-latency-benchmark-write";
  const size_t kObjectSizeBytes = 1_KB;
};

}  // namespace skyrise
