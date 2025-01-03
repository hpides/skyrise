#pragma once

#include "abstract_benchmark_runner.hpp"
#include "client/coordinator_client.hpp"
#include "system_benchmark_config.hpp"
#include "system_benchmark_result.hpp"
#include "utils/costs/cost_calculator.hpp"

namespace skyrise {

class SystemBenchmarkRunner : public AbstractBenchmarkRunner {
 public:
  SystemBenchmarkRunner(std::shared_ptr<const Aws::IAM::IAMClient> iam_client,
                        std::shared_ptr<const Aws::Lambda::LambdaClient> lambda_client,
                        std::shared_ptr<const Aws::S3::S3Client> s3_client,
                        std::shared_ptr<const CostCalculator> cost_calculator, const Aws::String& client_region,
                        const bool metering = true, const bool introspection = true);

  std::shared_ptr<SystemBenchmarkResult> RunSystemConfig(const std::shared_ptr<SystemBenchmarkConfig>& config);

 protected:
  void Setup() override;
  void Teardown() override;

  std::shared_ptr<AbstractBenchmarkResult> OnRunConfig() override;

  const std::shared_ptr<const Aws::IAM::IAMClient> iam_client_;
  const std::shared_ptr<const Aws::Lambda::LambdaClient> lambda_client_;
  const std::shared_ptr<const Aws::S3::S3Client> s3_client_;
  const std::shared_ptr<const CostCalculator> cost_calculator_;
  const Aws::String client_region_;

  std::string coordinator_function_name_;
  std::string worker_function_name_;
  std::string shuffle_storage_prefix_;

  std::shared_ptr<SystemBenchmarkConfig> typed_config_;
  std::shared_ptr<SystemBenchmarkResult> benchmark_result_;
};

}  // namespace skyrise
