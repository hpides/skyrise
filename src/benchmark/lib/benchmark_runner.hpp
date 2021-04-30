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

#include "benchmark_config.hpp"
#include "benchmark_result.hpp"
#include "client/client.hpp"

namespace skyrise {

class BenchmarkRunner {
 public:
  BenchmarkRunner(std::shared_ptr<Client> client);

  std::shared_ptr<BenchmarkResult> RunConfig(const BenchmarkConfig& config);

 private:
  void SetConfig(const BenchmarkConfig& config);

  void Setup();
  void SetupEventQueue();
  void Teardown();

  void InvokeFunctions();

  void WarmUpFunctions(const size_t repetition);
  void CreateInvokeRequests();
  void CollectSqsMessages(const size_t invocation_count);

  static Aws::Utils::CryptoBuffer OpenFunctionZip(const Aws::String& function_path);
  Aws::Lambda::Model::FunctionCode SetFunctionCode(const Aws::String& function_path, const Aws::String& function_name,
                                                   const bool is_local);

  std::shared_ptr<BenchmarkConfig> config_;
  std::unordered_set<Aws::String> config_history_;

  std::vector<std::vector<std::pair<Aws::String, Aws::Lambda::Model::InvokeRequest>>> invoke_requests_;

  const std::shared_ptr<Client> client_;

  std::shared_ptr<Aws::String> sqs_queue_url_;

  std::shared_ptr<BenchmarkResult> benchmark_result_;
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
