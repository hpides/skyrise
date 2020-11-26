#pragma once

#include <memory>
#include <tuple>

#include <aws/core/Aws.h>
#include <aws/core/utils/json/JsonSerializer.h>

#include "benchmark.hpp"
#include "benchmark_helper.hpp"
#include "benchmark_runner.hpp"
#include "utils/costs/cost_calculator.hpp"

namespace skyrise {

enum class S3OperationType { kRead, kWrite };

struct NetworkBenchmarkParameters {
  size_t function_instance_mb_size_;
  size_t object_byte_size_;
  size_t thread_count_;
  S3OperationType operation_type_;
};

class NetworkBenchmark : public Benchmark {
 public:
  Aws::Utils::Array<Aws::Utils::Json::JsonValue> Run(const std::shared_ptr<BenchmarkRunner>& benchmark_runner);

 protected:
  NetworkBenchmark(std::shared_ptr<BenchmarkHelper> helper, std::shared_ptr<CostCalculator> cost_calculator,
                   const size_t num_iterations, const ExecuteMode execute_mode, const Aws::String& read_bucket,
                   const Aws::String& write_bucket, const std::vector<size_t>& function_instance_mb_sizes,
                   const std::vector<size_t>& object_byte_sizes, const std::vector<size_t>& thread_counts);

  virtual Aws::Utils::Json::JsonValue GenerateResultOutput(
      const std::shared_ptr<std::vector<BenchmarkItemResult>>& result,
      const NetworkBenchmarkParameters& parameters) = 0;

  void Setup();
  void Teardown();

  Aws::String GenerateObjectKey(const bool is_parallel, const size_t objects_byte_size, const size_t thread_index,
                                const size_t iteration_index = 0);
  std::vector<std::shared_ptr<Aws::IOStream>> GeneratePayloads(const size_t function_instance_mb_size,
                                                               const size_t object_byte_size, const size_t thread_count,
                                                               const S3OperationType operation_type);

  long double ExtractFunctionCost(const BenchmarkItemResult& result, const size_t function_instance_mb_size);
  long double CalculateBenchmarkCost(const std::shared_ptr<std::vector<BenchmarkItemResult>>& result,
                                     const size_t function_instance_mb_size);

  const std::shared_ptr<BenchmarkHelper> helper_;
  const std::shared_ptr<CostCalculator> cost_calculator_;

  const size_t num_iterations_;
  const ExecuteMode execute_mode_;

  const Aws::String read_bucket_;
  const Aws::String write_bucket_;

  std::vector<std::tuple<BenchmarkConfig, NetworkBenchmarkParameters>> configs_;

  long double cost_overhead_;

  const Aws::String kFunctionName = "skyriseFunctionReadWriteS3";
  const Aws::String kObjectKeySuffix = "networkBenchmark";
};

}  // namespace skyrise
