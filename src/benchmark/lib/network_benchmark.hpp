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
  size_t function_instance_mb_size;
  size_t object_byte_size;
  size_t batch_size;
  size_t thread_count;
  size_t invocation_count;
  size_t bucket_count;
  S3OperationType operation_type;
};

class NetworkBenchmark : public Benchmark {
 public:
  Aws::Utils::Array<Aws::Utils::Json::JsonValue> Run(const std::shared_ptr<BenchmarkRunner>& benchmark_runner);

 protected:
  NetworkBenchmark(std::shared_ptr<BenchmarkHelper> helper, std::shared_ptr<CostCalculator> cost_calculator,
                   const std::vector<size_t>& bucket_counts = {1});

  virtual Aws::Utils::Json::JsonValue GenerateResultOutput(const std::shared_ptr<BenchmarkResult>& result,
                                                           const NetworkBenchmarkParameters& parameters) = 0;

  void Setup();
  void Teardown();

  static Aws::String GenerateObjectKey(const size_t object_byte_size, const size_t invocation_index,
                                       const size_t thread_index);
  static std::vector<std::shared_ptr<Aws::IOStream>> GeneratePayloads(const NetworkBenchmarkParameters& parameters);

  const std::shared_ptr<BenchmarkHelper> helper_;

  const std::vector<size_t> bucket_counts_;

  std::vector<std::pair<NetworkBenchmarkParameters, BenchmarkConfig>> benchmark_configs_;

  long double cost_overhead_;

  static constexpr size_t kMaxObjectsPerPrefix = 1000;
  const size_t kMaxMemoryUsageBytes = GbToByte(2);  // TODO(julianmenzler) C++20: Use consteval & constexpr
  inline static const Aws::String kBucketPrefix{"network-benchmark-"};
};

}  // namespace skyrise
