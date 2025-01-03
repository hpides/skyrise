#pragma once

#include <vector>

#include "lambda_benchmark.hpp"
#include "lambda_benchmark_config.hpp"
#include "lambda_benchmark_output.hpp"
#include "lambda_benchmark_types.hpp"
#include "monitoring/lambda_segments_analyzer.hpp"
#include "utils/costs/cost_calculator.hpp"

namespace skyrise {

class LambdaStartupLatencyBenchmark : public LambdaBenchmark {
 public:
  LambdaStartupLatencyBenchmark(std::shared_ptr<const CostCalculator> cost_calculator,
                                std::shared_ptr<const Aws::S3::S3Client> s3_client,
                                std::shared_ptr<const Aws::XRay::XRayClient> xray_client,
                                const std::vector<size_t>& function_instance_sizes_mb,
                                const std::vector<size_t>& concurrent_instance_counts, const size_t repetition_count,
                                const std::vector<DeploymentType>& deployment_types);

  const Aws::String& Name() const override;

 protected:
  Aws::Utils::Array<Aws::Utils::Json::JsonValue> OnRun(const std::shared_ptr<LambdaBenchmarkRunner>& runner) override;

 private:
  void Setup();

  void Teardown();

  static LambdaBenchmarkOutput& AddParameters(LambdaBenchmarkOutput& output,
                                              const LambdaStartupBenchmarkParameters& parameters);

  Aws::Utils::Json::JsonValue GenerateResultOutput(
      const std::shared_ptr<LambdaBenchmarkResult>& result,
      const std::shared_ptr<std::unordered_map<Aws::String, LambdaSegmentDurations>>& result_segments,
      const LambdaStartupBenchmarkParameters& parameters) const;

  const std::shared_ptr<const Aws::S3::S3Client> s3_client_;
  const std::shared_ptr<const Aws::XRay::XRayClient> xray_client_;

  const std::vector<size_t> function_instance_sizes_mb_;
  const std::vector<size_t> concurrent_instance_counts_;
  const size_t repetition_count_;
  const std::vector<DeploymentType> deployment_types_;

  std::vector<std::pair<LambdaStartupBenchmarkParameters, std::shared_ptr<LambdaBenchmarkConfig>>> configs_;
};

}  // namespace skyrise
