#pragma once

#include <chrono>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <aws/core/Aws.h>
#include <aws/lambda/model/InvokeRequest.h>
#include <aws/lambda/model/InvokeResult.h>

#include "benchmark_config.hpp"
#include "benchmark_result.hpp"
#include "client/client_aws.hpp"

namespace skyrise {

class BenchmarkRunner {
 public:
  BenchmarkRunner(std::shared_ptr<ClientAws> client_aws);

  std::shared_ptr<BenchmarkResult> RunConfig(const BenchmarkConfig& config);

  size_t setup_thread_count_ = 32;

 private:
  void SetConfig(const BenchmarkConfig& config);

  void Setup();
  void SetupAsync();
  void Teardown();

  void RunSequential();
  void RunParallel();

  void WarmUpFunctions();
  std::pair<Aws::String, Aws::Lambda::Model::InvokeRequest> CreateInvokeRequest(
      const Aws::String& function_name, const Aws::String& invocation_id, const size_t repetition, const bool is_warmup,
      const std::shared_ptr<Aws::IOStream>& payload = nullptr);
  void CreateInvokeRequests();
  void CreateWarmupInvokeRequests();
  std::shared_ptr<std::unordered_map<Aws::String, Aws::String>> CollectSqsMessages(const size_t invocation_count);

  static Aws::Utils::CryptoBuffer OpenFunctionZip(const Aws::String& function_path);
  std::vector<Aws::Lambda::Model::CreateFunctionOutcome> UploadFunctions(const size_t thread_count,
                                                                         const size_t thread_index);

  bool IsWarmStartBenchmark();
  bool IsAsyncBenchmark();
  bool IsParallelBenchmark();

  std::shared_ptr<BenchmarkConfig> config_;
  std::unordered_set<Aws::String> config_history_;

  std::vector<std::unordered_map<Aws::String, Aws::Lambda::Model::InvokeRequest>> invoke_requests_;
  std::vector<std::unordered_map<Aws::String, Aws::Lambda::Model::InvokeRequest>> invoke_warmup_requests_;

  const std::shared_ptr<ClientAws> client_aws_;

  std::shared_ptr<Aws::String> sqs_queue_url_;
  std::shared_ptr<std::unordered_map<Aws::String, Aws::String>> sqs_messages_;

  std::shared_ptr<BenchmarkResult> benchmark_result_;

  const Aws::String kFunctionRoleName = "AWSLambda";
  Aws::String function_role_arn_;
};

class ContextFunctionInvocation : public Aws::Client::AsyncCallerContext {
 public:
  ContextFunctionInvocation(const size_t repetition, const Aws::String& invocation_id)
      : Aws::Client::AsyncCallerContext(invocation_id), repetition_(repetition) {}
  size_t GetRepetition() const { return repetition_; }

 private:
  const size_t repetition_;
};

}  // namespace skyrise
