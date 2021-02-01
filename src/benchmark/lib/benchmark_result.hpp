#pragma once

#include <chrono>
#include <map>
#include <memory>
#include <mutex>
#include <tuple>
#include <vector>

#include <aws/core/Aws.h>
#include <aws/lambda/model/InvokeResult.h>

namespace skyrise {

struct InvocationResult {
  Aws::String invocation_id_;
  bool success_;
  bool finished_;
  Aws::String sqs_message_body_;
  std::shared_ptr<Aws::Lambda::Model::InvokeResult> invoke_result_;
  std::chrono::time_point<std::chrono::system_clock> start_point_;
  std::chrono::time_point<std::chrono::system_clock> end_point_;
};

class BenchmarkResult {
 public:
  BenchmarkResult(const size_t repetition_count, const size_t invocation_count);

  void RegisterInvocation(const size_t repetition, const Aws::String& invocation_id);
  void FinishInvocation(const size_t repetition, const Aws::String& invocation_id,
                        const std::shared_ptr<Aws::Lambda::Model::InvokeResult>& result, const bool success);
  void UpdateSQSMessageBody(const size_t repetition, const Aws::String& invocation_id,
                            const Aws::String& sqs_message_body);

  const std::vector<std::map<Aws::String, InvocationResult>>& GetInvocationResults();
  std::chrono::duration<double> GetRepetitionDuration(const size_t repetition);
  std::chrono::duration<double> GetBenchmarkDuration();

 private:
  std::vector<std::map<Aws::String, InvocationResult>> invocation_results_;
  std::vector<size_t> invocations_finished_;
  std::vector<std::tuple<std::chrono::time_point<std::chrono::system_clock>,
                         std::chrono::time_point<std::chrono::system_clock>>>
      repetition_durations_;
  size_t invocation_count_;

  std::chrono::time_point<std::chrono::system_clock> benchmark_start_point_;
  std::chrono::time_point<std::chrono::system_clock> benchmark_end_point_;

  std::mutex mutex_register_invocation_;
  std::mutex mutex_finish_invocation_;
};

}  // namespace skyrise
