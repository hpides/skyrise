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
#include "client/client.hpp"
#include "lambda_benchmark_config.hpp"
#include "lambda_benchmark_result.hpp"

namespace skyrise {

class LambdaBenchmarkRunner : public AbstractBenchmarkRunner {
 public:
  LambdaBenchmarkRunner(std::shared_ptr<const Aws::IAM::IAMClient> iam_client,
                        std::shared_ptr<const Aws::Lambda::LambdaClient> lambda_client,
                        std::shared_ptr<const Aws::SQS::SQSClient> sqs_client,
                        std::shared_ptr<const CostCalculator> cost_calculator);

  std::shared_ptr<LambdaBenchmarkResult> RunLambdaConfig(const std::shared_ptr<LambdaBenchmarkConfig>& config);

 protected:
  void Setup() override;
  void SetupEventQueue();
  void Teardown() override;

  std::shared_ptr<AbstractBenchmarkResult> OnRunConfig() override;

  void WarmUpFunctions(const size_t repetition);
  void CreateInvokeRequests();
  void CollectSqsMessages(const size_t invocation_count);

  static Aws::Utils::CryptoBuffer OpenFunctionZip(const Aws::String& function_path);
  Aws::Lambda::Model::FunctionCode SetFunctionCode(const Aws::String& function_path, const Aws::String& function_name,
                                                   const bool is_local);

  std::shared_ptr<LambdaBenchmarkConfig> typed_config_;

  std::vector<std::vector<std::pair<Aws::String, Aws::Lambda::Model::InvokeRequest>>> invoke_requests_;

  const std::shared_ptr<const Aws::IAM::IAMClient> iam_client_;
  const std::shared_ptr<const Aws::Lambda::LambdaClient> lambda_client_;
  const std::shared_ptr<const Aws::SQS::SQSClient> sqs_client_;
  const std::shared_ptr<const CostCalculator> cost_calculator_;

  std::shared_ptr<Aws::String> sqs_queue_url_;

  std::shared_ptr<LambdaBenchmarkResult> benchmark_result_;
  std::unordered_map<std::string, Aws::Utils::CryptoBuffer> package_files_;
  std::mutex package_files_mutex_;

  const Aws::String kFunctionRoleName{"AWSLambda"};
  Aws::String function_role_arn_;
};

class ContextFunctionInvocation : public Aws::Client::AsyncCallerContext {
 public:
  ContextFunctionInvocation(const size_t repetition, const size_t invoke_index, const Aws::String& invocation_id)
      : Aws::Client::AsyncCallerContext(invocation_id), repetition_(repetition), invoke_index_(invoke_index) {}
  size_t GetRepetition() const { return repetition_; }
  size_t GetInvokeIndex() const { return invoke_index_; }

 private:
  const size_t repetition_;
  const size_t invoke_index_;
};

}  // namespace skyrise
