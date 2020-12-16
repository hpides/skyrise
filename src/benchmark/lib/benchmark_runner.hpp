#pragma once

#include <chrono>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <aws/core/Aws.h>
#include <aws/lambda/model/InvokeRequest.h>
#include <aws/lambda/model/InvokeResult.h>

#include "benchmark_config.hpp"
#include "client/client_aws.hpp"

namespace skyrise {

struct BenchmarkItemResult {
  Aws::String invocation_id;
  Aws::Lambda::Model::InvokeRequest invoke_request;
  bool success;
  std::chrono::time_point<std::chrono::steady_clock> start_time;
  std::chrono::time_point<std::chrono::steady_clock> end_time;
  std::shared_ptr<Aws::Lambda::Model::InvokeResult> invoke_result;
  Aws::String sqs_message_body;
  // TODO: Collect billed duration, tier1/2-requests, cost etc.
  // TODO: Introduce warmstart flag for early exit in warm up function invocation
  // TODO: Introduce host details flag for collection of host details during function invocation
};

class BenchmarkRunner {
 public:
  BenchmarkRunner(std::shared_ptr<ClientAws> client_aws);

  std::shared_ptr<std::vector<BenchmarkItemResult>> RunConfig(const BenchmarkConfig& config);

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
  std::shared_ptr<std::unordered_map<Aws::String, Aws::Lambda::Model::InvokeRequest>> CreateInvokeRequests();
  std::shared_ptr<std::unordered_map<Aws::String, Aws::Lambda::Model::InvokeRequest>> CreateWarmupInvokeRequests();
  std::shared_ptr<std::unordered_map<Aws::String, Aws::String>> CollectSqsMessages(const size_t invocation_count);

  static Aws::Utils::CryptoBuffer OpenFunctionZip(const Aws::String& function_path);
  std::vector<Aws::Lambda::Model::CreateFunctionOutcome> UploadFunctions(const size_t thread_count,
                                                                         const size_t thread_index);
  BenchmarkItemResult RunBenchmarkItem(const Aws::String& invocation_id,
                                       const Aws::Lambda::Model::InvokeRequest& invoke_request);
  void WriteResult(const std::shared_ptr<std::vector<BenchmarkItemResult>>& benchmark_item_results,
                   const std::chrono::duration<size_t, std::milli> benchmark_run_duration);

  bool IsWarmStartBenchmark();
  bool IsAsyncBenchmark();
  bool IsParallelBenchmark();

  std::shared_ptr<BenchmarkConfig> config_;
  std::unordered_set<Aws::String> config_history_;

  std::shared_ptr<std::unordered_map<Aws::String, Aws::Lambda::Model::InvokeRequest>> invoke_requests_;
  std::shared_ptr<std::unordered_map<Aws::String, Aws::Lambda::Model::InvokeRequest>> invoke_warmup_requests_;

  const std::shared_ptr<ClientAws> client_aws_;

  std::shared_ptr<Aws::String> sqs_queue_url_;
  std::shared_ptr<std::unordered_map<Aws::String, Aws::String>> sqs_messages_;

  std::shared_ptr<std::vector<BenchmarkItemResult>> result_;

  const Aws::String kFunctionRoleName = "AWSLambda";
  Aws::String function_role_arn_;
};

}  // namespace skyrise
