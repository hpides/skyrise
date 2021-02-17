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
  size_t function_instance_mb_size;
  size_t invocation_count;
  bool warm_mode;
  size_t repetition_count;
};

class InvocationLatencyBenchmark : public Benchmark {
 public:
  InvocationLatencyBenchmark(std::shared_ptr<Client> client, std::shared_ptr<BenchmarkHelper> helper,
                             std::shared_ptr<CostCalculator> cost_calculator_,
                             const std::vector<size_t>& function_instance_mb_sizes,
                             const std::vector<size_t>& invocation_counts, const std::vector<bool>& warm_modes,
                             const size_t repetition_count);

  Aws::Utils::Array<Aws::Utils::Json::JsonValue> Run(const std::shared_ptr<BenchmarkRunner>& benchmark_runner) override;

 private:
  void Setup();
  void Teardown();
  long double CalculateBenchmarkCost(const std::vector<std::shared_ptr<BenchmarkResult>>& benchmark_results);
  static Aws::String ExtractTraceId(const InvocationResult& result);
  Aws::Utils::Json::JsonValue GenerateResultOutput(
      const std::shared_ptr<BenchmarkResult>& benchmark_result, const InvocationLatencyBenchmarkParameters& parameters,
      const std::shared_ptr<std::unordered_map<Aws::String, LambdaSegmentDurations>>& result_segments) const;

  const std::shared_ptr<Client> client_;
  const std::shared_ptr<BenchmarkHelper> helper_;
  const std::shared_ptr<CostCalculator> cost_calculator_;
  const std::vector<size_t> function_instance_mb_sizes_;
  const std::vector<size_t>& invocation_counts_;
  const std::vector<bool>& warm_modes_;
  const size_t repetition_count_;

  long double benchmark_cost_;
  long double cost_overhead_;
  std::shared_ptr<FunctionSegmentsAnalyzer> function_segments_analyzer_;
  std::vector<std::pair<InvocationLatencyBenchmarkParameters, BenchmarkConfig>> benchmark_configs_;

  const std::vector<Aws::String> kPackageNames{
      "skyriseFunctionMinimal",      "S3_skyriseFunctionMinimal",   "skyriseFunctionSized10MB",
      "S3_skyriseFunctionSized10MB", "skyriseFunctionSized20MB",    "S3_skyriseFunctionSized20MB",
      "skyriseFunctionSized30MB",    "S3_skyriseFunctionSized30MB", "skyriseFunctionSized40MB",
      "S3_skyriseFunctionSized40MB", "skyriseFunctionSized50MB",    "S3_skyriseFunctionSized50MB",
      "S3_skyriseFunctionSized100MB"};

  const Aws::String kBenchmarkName = "invocation-latency-benchmark";
  const bool kEnableTracing = true;
  const double kOverprovisioningCoefficient = 1.2;
  const Aws::String kTag = "SKYRISE/BENCHMARK/INVOCATION_LATENCY_BENCHMARK";
  const size_t kTraceRetrievalDelayMs = 5;
};

}  // namespace skyrise
