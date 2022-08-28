#pragma once

#include <vector>

#include "benchmark_helper.hpp"
#include "lambda_benchmark.hpp"
#include "lambda_benchmark_config.hpp"
#include "monitoring/function_segments.hpp"
#include "utils/costs/cost_calculator.hpp"

namespace skyrise {

struct InvocationLatencyBenchmarkParameters {
  size_t function_instance_mb_size;
  size_t invocation_count;
  bool warm_mode;
  size_t sleep_ms_duration;
  size_t repetition_count;
  Aws::String function_package_name;
};

class InvocationLatencyBenchmark : public LambdaBenchmark {
 public:
  InvocationLatencyBenchmark(std::shared_ptr<const Aws::XRay::XRayClient> xray_client,
                             std::shared_ptr<const BenchmarkHelper> helper,
                             std::shared_ptr<const CostCalculator> cost_calculator,
                             const std::vector<size_t>& function_instance_mb_sizes,
                             const std::vector<size_t>& invocation_counts, const std::vector<bool>& warm_modes,
                             const std::vector<size_t>& sleep_ms_durations, const size_t repetition_count);
  const Aws::String& Name() const override;

 protected:
  Aws::Utils::Array<Aws::Utils::Json::JsonValue> OnRun(
      const std::shared_ptr<LambdaBenchmarkRunner>& benchmark_runner) override;

 private:
  void Setup();
  void Teardown();
  long double CalculateBenchmarkCost(const std::vector<std::shared_ptr<LambdaBenchmarkResult>>& benchmark_results);
  Aws::Utils::Json::JsonValue GenerateResultOutput(
      const std::shared_ptr<LambdaBenchmarkResult>& benchmark_result,
      const InvocationLatencyBenchmarkParameters& parameters,
      const std::shared_ptr<std::unordered_map<Aws::String, LambdaSegmentDurations>>& result_segments) const;

  const std::shared_ptr<const Aws::XRay::XRayClient> xray_client_;
  const std::shared_ptr<const BenchmarkHelper> helper_;
  const std::shared_ptr<const CostCalculator> cost_calculator_;
  const std::vector<size_t> function_instance_mb_sizes_;
  const std::vector<size_t> invocation_counts_;
  const std::vector<bool> warm_modes_;
  const std::vector<size_t> sleep_ms_durations_;
  const size_t repetition_count_;

  long double benchmark_cost_;
  std::shared_ptr<FunctionSegmentsAnalyzer> function_segments_analyzer_;
  std::vector<std::pair<InvocationLatencyBenchmarkParameters, std::shared_ptr<LambdaBenchmarkConfig>>>
      benchmark_configs_;

  const std::vector<Aws::String> kPackageNames{
      "skyriseFunctionMinimal",      "S3_skyriseFunctionMinimal",   "skyriseFunctionSized10MB",
      "S3_skyriseFunctionSized10MB", "skyriseFunctionSized20MB",    "S3_skyriseFunctionSized20MB",
      "skyriseFunctionSized30MB",    "S3_skyriseFunctionSized30MB", "skyriseFunctionSized40MB",
      "S3_skyriseFunctionSized40MB", "skyriseFunctionSized50MB",    "S3_skyriseFunctionSized50MB",
      "S3_skyriseFunctionSized100MB"};

  static constexpr bool kEnableTracing = true;
  static constexpr double kOverprovisioningCoefficient = 1.2;
  static constexpr size_t kTraceRetrievalDelayMs = 5;
  inline static const Aws::String kBenchmarkName{"invocation-latency-benchmark"};
  inline static const Aws::String kTag{"SKYRISE/BENCHMARK/INVOCATION_LATENCY_BENCHMARK"};
};

}  // namespace skyrise
