#include "benchmark_result.hpp"

#include <numeric>

namespace skyrise {

BenchmarkResult::BenchmarkResult(const size_t repetition_count, const size_t invocation_count)
    : invocation_results_(repetition_count),
      invocations_finished_(repetition_count),
      function_warm_up_costs_(repetition_count),
      invocation_count_(invocation_count) {
  repetition_durations_.reserve(repetition_count);
  benchmark_start_point_ = std::chrono::system_clock::now();
}

void BenchmarkResult::RegisterInvocation(const size_t repetition, const Aws::String& invocation_id) {
  const auto now = std::chrono::system_clock::now();

  std::lock_guard<std::mutex> lock(mutex_register_invocation_);

  if (invocation_results_[repetition].empty()) {
    repetition_durations_[repetition] = {now, now};
  }

  invocation_results_[repetition][invocation_id] = {invocation_id, false, false, {}, nullptr, now, now};
}

void BenchmarkResult::FinishInvocation(const size_t repetition, const Aws::String& invocation_id,
                                       const std::shared_ptr<Aws::Lambda::Model::InvokeResult>& result,
                                       const bool success) {
  const auto now = std::chrono::system_clock::now();

  auto& benchmark_item_result = invocation_results_[repetition][invocation_id];

  benchmark_item_result.end_point_ = now;
  benchmark_item_result.success_ = success;
  benchmark_item_result.finished_ = true;
  benchmark_item_result.invoke_result_ = result;

  std::lock_guard<std::mutex> lock(mutex_finish_invocation_);

  invocations_finished_[repetition]++;

  if (invocations_finished_[repetition] == invocation_count_) {
    std::get<1>(repetition_durations_[repetition]) = now;

    if (repetition == invocation_results_.size() - 1) {
      benchmark_end_point_ = now;
    }
  }
}

void BenchmarkResult::UpdateSQSMessageBody(const size_t repetition, const Aws::String& invocation_id,
                                           const Aws::String& sqs_message_body) {
  invocation_results_[repetition][invocation_id].sqs_message_body_ = sqs_message_body;
}

void BenchmarkResult::SetFunctionWarmUpCost(const size_t repetition, const long double cost) {
  function_warm_up_costs_[repetition] = cost;
}

const std::vector<std::map<Aws::String, InvocationResult>>& BenchmarkResult::GetInvocationResults() const {
  return invocation_results_;
}

std::chrono::duration<double> BenchmarkResult::GetRepetitionDuration(const size_t repetition) const {
  const auto& [start, end] = repetition_durations_[repetition];
  return std::chrono::duration<double>(end - start);
}

std::chrono::duration<double> BenchmarkResult::GetBenchmarkDuration() const {
  return std::chrono::duration<double>(benchmark_end_point_ - benchmark_start_point_);
}

const std::vector<long double>& BenchmarkResult::GetFunctionWarmUpCosts() const { return function_warm_up_costs_; }

long double BenchmarkResult::GetOverallFunctionWarmUpCost() const {
  return std::accumulate(function_warm_up_costs_.cbegin(), function_warm_up_costs_.cend(), 0.0L);
}

}  // namespace skyrise
