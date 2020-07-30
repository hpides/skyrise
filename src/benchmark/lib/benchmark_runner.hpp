#pragma once

#include <chrono>
#include <map>
#include <vector>

#include <aws/core/Aws.h>
#include <aws/core/utils/Outcome.h>
#include <aws/iam/IAMClient.h>
#include <aws/lambda/LambdaClient.h>
#include <aws/lambda/model/InvokeRequest.h>
#include <aws/lambda/model/InvokeResult.h>
#include <aws/sqs/SQSClient.h>

#include "benchmark_config.hpp"

namespace skyrise {

struct BenchmarkItemResult {
  Aws::String invocation_id;
  Aws::Lambda::Model::InvokeRequest invoke_request;
  bool success;
  std::chrono::time_point<std::chrono::steady_clock> start_time;
  std::chrono::time_point<std::chrono::steady_clock> end_time;
  std::shared_ptr<Aws::Lambda::Model::InvokeOutcome> outcome;
  std::shared_ptr<Aws::String> sqs_message_body;
  // TODO: Collect billed duration, tier1/2-requests, cost etc.
  // TODO: Introduce warmstart flag for early exit in warm up function invocation
  // TODO: Introduce host details flag for collection of host details during function invocation
};

class BenchmarkRunner {
 public:
  BenchmarkRunner(const BenchmarkConfig& config);

  void Run();

  const std::shared_ptr<std::vector<BenchmarkItemResult>> GetBenchmarkResult() const;

 private:
  void Setup();
  void SetupAsync();
  void Teardown();

  void RunSequential();
  void RunParallel();

  void WarmUpFunctions();
  std::shared_ptr<std::map<Aws::String, Aws::Lambda::Model::InvokeRequest>> CreateInvokeRequests(
      const bool with_warm_up_suffix);
  std::shared_ptr<std::map<Aws::String, Aws::String>> CollectSqsMessages(const size_t num_invocations);

  Aws::Utils::CryptoBuffer OpenFunctionZip(const Aws::String& function_path);
  BenchmarkItemResult RunBenchmarkItem(const Aws::String& invocation_id,
                                       const Aws::Lambda::Model::InvokeRequest& invoke_request);
  void WriteResult(const std::shared_ptr<std::vector<BenchmarkItemResult>> benchmark_item_results,
                   const std::chrono::duration<size_t, std::milli> benchmark_run_duration);

  bool IsWarmStartBenchmark();
  bool IsAsyncBenchmark();
  bool IsParallelBenchmark();

  const BenchmarkConfig config_;
  std::shared_ptr<std::map<Aws::String, Aws::Lambda::Model::InvokeRequest>> invoke_requests_;
  std::shared_ptr<std::map<Aws::String, Aws::Lambda::Model::InvokeRequest>> invoke_warmup_requests_;

  Aws::IAM::IAMClient iam_client_;
  Aws::Lambda::LambdaClient lambda_client_;
  Aws::SQS::SQSClient sqs_client_;

  std::shared_ptr<Aws::String> sqs_queue_url_;
  std::shared_ptr<std::map<Aws::String, Aws::String>> sqs_messages_;

  std::shared_ptr<std::vector<BenchmarkItemResult>> result_;
};

}  // namespace skyrise
