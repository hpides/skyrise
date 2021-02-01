#pragma once

#include <vector>

#include "benchmark.hpp"
#include "benchmark_config.hpp"
#include "benchmark_helper.hpp"
#include "function_segments.hpp"
#include "utils/costs/cost_calculator.hpp"

namespace skyrise {

struct InvocationLatencyBenchmarkParameters {
  Aws::String function_package_name;
  size_t function_size;
};

class InvocationLatencyBenchmark : public Benchmark {
 public:
  InvocationLatencyBenchmark(std::shared_ptr<Client> client, std::shared_ptr<BenchmarkHelper> helper,
                             std::shared_ptr<CostCalculator> cost_calculator_,
                             const std::vector<size_t>& function_instance_mb_sizes, size_t repetition_count,
                             size_t invocation_count, bool warm_mode);
  Aws::Utils::Array<Aws::Utils::Json::JsonValue> Run(const std::shared_ptr<BenchmarkRunner>& benchmark_runner) override;

 private:
  void Setup();
  void Teardown();
  long double CalculateBenchmarkCost(const std::vector<std::shared_ptr<BenchmarkResult>>& benchmark_results);
  long double ExtractFunctionCost(const InvocationResult& result, const size_t lambda_size);
  static Aws::String ExtractTraceId(const InvocationResult& result);
  Aws::Utils::Json::JsonValue GenerateResultOutput(
      const std::shared_ptr<BenchmarkResult>& benchmark_result, const InvocationLatencyBenchmarkParameters& parameters,
      const std::shared_ptr<std::unordered_map<Aws::String, LambdaSegmentDurations>>& result_segments) const;

  const std::shared_ptr<Client> client_;
  const std::shared_ptr<BenchmarkHelper> helper_;
  const std::shared_ptr<CostCalculator> cost_calculator_;
  const std::vector<size_t> function_instance_mb_sizes_;
  const size_t invocation_count_;
  const size_t repetition_count_;
  const bool warm_mode_;

  std::vector<std::pair<BenchmarkConfig, InvocationLatencyBenchmarkParameters>> configs_;
  std::shared_ptr<FunctionSegmentsAnalyzer> function_segments_analyzer_;
  long double benchmark_cost_;
  long double cost_overhead_;

  const std::vector<Aws::String> kPackageNames{
      "skyriseFuncInvocLat",        "S3_skyriseFuncInvocLat",     "skyriseFuncInvocLat10MB",
      "S3_skyriseFuncInvocLat10MB", "skyriseFuncInvocLat20MB",    "S3_skyriseFuncInvocLat20MB",
      "skyriseFuncInvocLat30MB",    "S3_skyriseFuncInvocLat30MB", "skyriseFuncInvocLat40MB",
      "S3_skyriseFuncInvocLat40MB", "skyriseFuncInvocLat50MB",    "S3_skyriseFuncInvocLat50MB",
      "S3_skyriseFuncInvocLat100MB"};

  const Aws::String kBenchmarkName = "invocation-latency-benchmark";
  const bool kEnableTracing = true;
  const double kOverprovisioningCoefficient = 1.2;
  const Aws::String kTag = "SKYRISE/BENCHMARK/INVOCATION_LATENCY_BENCHMARK";
  const size_t kTraceRetrievalDelayMs = 5;
};

}  // namespace skyrise
