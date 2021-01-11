#pragma once

#include <memory>
#include <tuple>

#include <aws/core/Aws.h>
#include <aws/core/utils/json/JsonSerializer.h>

#include "benchmark.hpp"
#include "benchmark_helper.hpp"
#include "benchmark_runner.hpp"
#include "utils/costs/cost_calculator.hpp"
#include "utils/unit_conversion.hpp"

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
                   const size_t repetition_count, const size_t batch_size, const std::vector<size_t>& object_byte_sizes,
                   const std::vector<size_t>& thread_counts, const std::vector<size_t>& concurrent_invocation_counts);

  virtual Aws::Utils::Json::JsonValue GenerateResultOutput(
      const std::shared_ptr<std::vector<BenchmarkItemResult>>& result,
      const NetworkBenchmarkParameters& parameters) = 0;

  void Setup();
  void Teardown();

  Aws::String GenerateObjectKey(const size_t object_byte_size, const size_t invocation_index,
                                const size_t thread_index) const;
  std::vector<std::shared_ptr<Aws::IOStream>> GeneratePayloads(const size_t function_instance_mb_size,
                                                               const size_t object_byte_size, const size_t thread_count,
                                                               const size_t invocation_count,
                                                               const S3OperationType operation_type);

  long double ExtractFunctionCost(const BenchmarkItemResult& result, const size_t function_instance_mb_size);
  long double CalculateBenchmarkCost(const std::shared_ptr<std::vector<BenchmarkItemResult>>& result,
                                     const size_t function_instance_mb_size);

  Aws::Utils::Array<Aws::Utils::Json::JsonValue> GenerateBatchedSubResultOutput(
      const std::shared_ptr<std::vector<BenchmarkItemResult>>& result, const Aws::String& benchmark_name,
      const size_t function_instance_mb_size, const Aws::String& metric_name,
      const std::function<double(const double)>& process_value);
  std::vector<double> ExtractValuesFromBatchedSubResults(
      const Aws::Utils::Array<Aws::Utils::Json::JsonValue>& batched_runs, const Aws::String& metric_name) const;

  const std::shared_ptr<BenchmarkHelper> helper_;
  const std::shared_ptr<CostCalculator> cost_calculator_;

  const size_t repetition_count_;
  const size_t batch_size_;

  std::vector<size_t> object_byte_sizes_;
  std::vector<size_t> thread_counts_;
  std::vector<size_t> concurrent_invocation_counts_;

  std::vector<std::tuple<BenchmarkConfig, NetworkBenchmarkParameters>> configs_;

  long double cost_overhead_;

  const size_t kMaxObjectsPerPrefix = 1000;
  const size_t kMaxMemoryUsageBytes = GbToByte(2);
  const Aws::String kObjectKeySuffix = "networkBenchmark";
  const Aws::String kReadBucket = "network-benchmark-read";
  const Aws::String kWriteBucket = "network-benchmark-write";
};

}  // namespace skyrise
