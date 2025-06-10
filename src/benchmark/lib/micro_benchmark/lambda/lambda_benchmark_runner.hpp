#pragma once

#include <chrono>
#include <mutex>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <aws/core/Aws.h>
#include <aws/lambda/model/FunctionCode.h>
#include <aws/lambda/model/InvokeRequest.h>
#include <aws/lambda/model/InvokeResult.h>
#include <aws/lambda/model/TracingConfig.h>

#include "abstract_benchmark_runner.hpp"
#include "client/coordinator_client.hpp"
#include "configuration.hpp"
#include "lambda_benchmark_config.hpp"
#include "lambda_benchmark_result.hpp"

namespace skyrise {

class LambdaBenchmarkRunner : public AbstractBenchmarkRunner {
 public:
  LambdaBenchmarkRunner(std::shared_ptr<const Aws::IAM::IAMClient> iam_client,
                        std::shared_ptr<const Aws::Lambda::LambdaClient> lambda_client,
                        std::shared_ptr<const Aws::SQS::SQSClient> sqs_client,
                        std::shared_ptr<const CostCalculator> cost_calculator, const Aws::String& client_region,
                        const bool metering = true, const bool introspection = true);

  std::shared_ptr<LambdaBenchmarkResult> RunLambdaConfig(const std::shared_ptr<LambdaBenchmarkConfig>& config);

 protected:
  void Setup() override;
  void SetupEventQueue();
  void Teardown() override;

  std::shared_ptr<AbstractBenchmarkResult> OnRunConfig() override;

  void WarmupFunctions(const size_t repetition);
  void CreateInvocationRequests();
  void CollectSqsMessages(const size_t invocation_count);

  std::shared_ptr<LambdaBenchmarkConfig> typed_config_;

  std::vector<std::vector<std::pair<Aws::String, Aws::Lambda::Model::InvokeRequest>>> invocation_requests_;

  const std::shared_ptr<const Aws::IAM::IAMClient> iam_client_;
  const std::shared_ptr<const Aws::Lambda::LambdaClient> lambda_client_;
  const std::shared_ptr<const Aws::SQS::SQSClient> sqs_client_;
  const std::shared_ptr<const CostCalculator> cost_calculator_;
  const Aws::String client_region_;

  std::unordered_map<std::string, Aws::Utils::CryptoBuffer> package_files_;
  std::mutex package_files_mutex_;

  std::shared_ptr<Aws::String> sqs_queue_url_;
  std::shared_ptr<LambdaBenchmarkResult> benchmark_result_;
};

class ContextFunctionInvocation : public Aws::Client::AsyncCallerContext {
 public:
  ContextFunctionInvocation(const size_t repetition, const size_t invocation, const Aws::String& invocation_id)
      : Aws::Client::AsyncCallerContext(invocation_id), repetition_(repetition), invocation_(invocation) {}
  size_t GetRepetition() const { return repetition_; }
  size_t GetInvocation() const { return invocation_; }

 private:
  const size_t repetition_;
  const size_t invocation_;
};

}  // namespace skyrise
