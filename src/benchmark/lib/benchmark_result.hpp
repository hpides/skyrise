#pragma once

#include <chrono>
#include <map>
#include <memory>
#include <mutex>
#include <tuple>
#include <vector>

#include <aws/core/Aws.h>
#include <aws/lambda/LambdaClient.h>

#include "log_result.hpp"

namespace skyrise {

class InvokeResult {
 public:
  explicit InvokeResult(Aws::String invoke_id = {});

  void Complete(Aws::Lambda::Model::InvokeOutcome* invoke_outcome);
  void UpdateSQSMessageBody(const Aws::String& sqs_message_body);

  const Aws::String& GetInvokeId() const;
  Aws::Utils::Json::JsonView GetResponseBody() const;
  std::shared_ptr<const LogResult> GetLogResult() const;

  double GetDurationMs() const;
  const std::chrono::time_point<std::chrono::system_clock>& GetStartPoint() const;
  const std::chrono::time_point<std::chrono::system_clock>& GetEndPoint() const;

  bool HasLogResult() const;

  bool IsSuccess() const;
  bool IsComplete() const;

 private:
  Aws::String invoke_id_;
  bool success_;
  bool complete_;

  std::chrono::time_point<std::chrono::system_clock> start_point_;
  std::chrono::time_point<std::chrono::system_clock> end_point_;

  Aws::Utils::Json::JsonValue response_body_;
  std::shared_ptr<const LogResult> log_result_;
};

class BenchmarkRepetition {
 public:
  explicit BenchmarkRepetition(const size_t invocation_count);
  void RegisterInvocation(const size_t invoke_index, const Aws::String& invoke_id);
  void CompleteInvocation(const size_t invoke_index, Aws::Lambda::Model::InvokeOutcome* invoke_outcome);
  void UpdateSQSMessageBody(const size_t invoke_index, const Aws::String& sqs_message_body);
  void SetFunctionWarmUpCost(const long double cost);

  const std::vector<InvokeResult>& GetInvokeResults() const;
  long double GetWarmUpCost() const;
  double GetDurationMs() const;
  bool IsComplete() const;

 private:
  const size_t invocation_count_;
  std::vector<InvokeResult> invoke_results_;

  long double warm_up_cost_;
};

class BenchmarkResult {
 public:
  BenchmarkResult(const size_t repetition_count, const size_t invocation_count);

  void RegisterInvocation(const size_t repetition, const size_t invoke_index, const Aws::String& invoke_id);
  void FinishInvocation(const size_t repetition, const size_t invoke_index,
                        Aws::Lambda::Model::InvokeOutcome* invoke_outcome);
  void UpdateSQSMessageBody(const size_t repetition, const size_t invoke_index, const Aws::String& sqs_message_body);
  void SetFunctionWarmUpCost(const size_t repetition, const long double cost);

  double GetDurationMs() const;
  const std::vector<BenchmarkRepetition>& GetBenchmarkRepetitions() const;
  long double GetWarmUpCost() const;

  bool HasRepetitionFinished(const size_t repetition) const;
  bool IsComplete() const;

 private:
  const size_t repetition_count_;
  const size_t invocation_count_;

  std::vector<BenchmarkRepetition> benchmark_repetitions_;
};

}  // namespace skyrise
