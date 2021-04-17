#include "benchmark_result.hpp"

#include <algorithm>
#include <numeric>

#include "utils/assert.hpp"
#include "utils/string.hpp"

namespace skyrise {

InvokeResult::InvokeResult(Aws::String invoke_id)
    : invoke_id_(std::move(invoke_id)),
      success_(false),
      complete_(false),
      start_point_(std::chrono::system_clock::now()),
      end_point_(start_point_) {}

void InvokeResult::Complete(Aws::Lambda::Model::InvokeOutcome* invoke_outcome) {
  end_point_ = std::chrono::system_clock::now();

  Assert(!complete_, "InvokeResult " + invoke_id_ + " is already complete.");
  complete_ = true;
  success_ = invoke_outcome != nullptr && invoke_outcome->IsSuccess();

  if (success_) {
    auto invoke_result = invoke_outcome->GetResultWithOwnership();
    response_body_ = Aws::Utils::Json::JsonValue(StreamToString(&invoke_result.GetPayload()));

    if (!invoke_result.GetLogResult().empty()) {
      log_result_ = std::make_shared<const LogResult>(invoke_result.GetLogResult());
    }
  }
}

void InvokeResult::UpdateSQSMessageBody(const Aws::String& sqs_message_body) {
  Assert(complete_, "InvokeResult " + invoke_id_ + " must be complete before updating the SQS message.");
  const auto sqs_message_body_value = Aws::Utils::Json::JsonValue(sqs_message_body);

  const auto sqs_message_body_view = sqs_message_body_value.View();
  Assert(sqs_message_body_view.KeyExists("responsePayload"), "SQS message must contain a response payload.");

  response_body_ = sqs_message_body_view.GetObject("responsePayload").Materialize();
}

const Aws::String& InvokeResult::GetInvokeId() const { return invoke_id_; }

Aws::Utils::Json::JsonView InvokeResult::GetResponseBody() const { return response_body_.View(); }

std::shared_ptr<const LogResult> InvokeResult::GetLogResult() const { return log_result_; }

double InvokeResult::GetDurationMs() const {
  Assert(complete_, "InvokeResult " + invoke_id_ + " must be complete before a duration is available.");
  return std::chrono::duration<double, std::milli>(end_point_ - start_point_).count();
}

const std::chrono::time_point<std::chrono::system_clock>& InvokeResult::GetStartPoint() const { return start_point_; }

const std::chrono::time_point<std::chrono::system_clock>& InvokeResult::GetEndPoint() const { return end_point_; }

bool InvokeResult::HasLogResult() const { return log_result_ != nullptr; }

bool InvokeResult::IsSuccess() const { return success_; }

bool InvokeResult::IsComplete() const { return complete_; }

BenchmarkRepetition::BenchmarkRepetition(const size_t invocation_count)
    : invocation_count_(invocation_count),
      invoke_results_(std::vector<InvokeResult>(invocation_count_)),
      warm_up_cost_(0) {}

void BenchmarkRepetition::RegisterInvocation(const size_t invoke_index, const Aws::String& invoke_id) {
  invoke_results_[invoke_index] = InvokeResult(invoke_id);
}

void BenchmarkRepetition::CompleteInvocation(const size_t invoke_index,
                                             Aws::Lambda::Model::InvokeOutcome* invoke_outcome) {
  invoke_results_[invoke_index].Complete(invoke_outcome);
}

void BenchmarkRepetition::UpdateSQSMessageBody(const size_t invoke_index, const Aws::String& sqs_message_body) {
  invoke_results_[invoke_index].UpdateSQSMessageBody(sqs_message_body);
}

void BenchmarkRepetition::SetFunctionWarmUpCost(const long double cost) { warm_up_cost_ = cost; }

const std::vector<InvokeResult>& BenchmarkRepetition::GetInvokeResults() const { return invoke_results_; }

long double BenchmarkRepetition::GetWarmUpCost() const { return warm_up_cost_; }

double BenchmarkRepetition::GetDurationMs() const {
  Assert(IsComplete(), "BenchmarkRepetition must be completed before a duration is available.");

  const auto start_point_minimum = std::min_element(
      invoke_results_.cbegin(), invoke_results_.cend(),
      [](const InvokeResult& a, const InvokeResult& b) { return a.GetStartPoint() < b.GetStartPoint(); });

  const auto end_point_maximum =
      std::max_element(invoke_results_.cbegin(), invoke_results_.cend(),
                       [](const InvokeResult& a, const InvokeResult& b) { return a.GetEndPoint() < b.GetEndPoint(); });

  return std::chrono::duration<double, std::milli>(end_point_maximum->GetEndPoint() -
                                                   start_point_minimum->GetStartPoint())
      .count();
}

bool BenchmarkRepetition::IsComplete() const {
  return std::all_of(invoke_results_.cbegin(), invoke_results_.cend(),
                     [](const InvokeResult& invoke_result) { return invoke_result.IsComplete(); });
}

BenchmarkResult::BenchmarkResult(const size_t repetition_count, const size_t invocation_count)
    : repetition_count_(repetition_count),
      invocation_count_(invocation_count),
      benchmark_repetitions_(repetition_count_, BenchmarkRepetition(invocation_count_)) {
  Assert(repetition_count > 0, "Repetition count must be greater than zero.");
  Assert(invocation_count > 0, "Invocation count must be greater than zero.");
}

void BenchmarkResult::RegisterInvocation(const size_t repetition, const size_t invoke_index,
                                         const Aws::String& invoke_id) {
  benchmark_repetitions_[repetition].RegisterInvocation(invoke_index, invoke_id);
}

void BenchmarkResult::FinishInvocation(const size_t repetition, const size_t invoke_index,
                                       Aws::Lambda::Model::InvokeOutcome* invoke_outcome) {
  benchmark_repetitions_[repetition].CompleteInvocation(invoke_index, invoke_outcome);
}
void BenchmarkResult::UpdateSQSMessageBody(const size_t repetition, const size_t invoke_index,
                                           const Aws::String& sqs_message_body) {
  benchmark_repetitions_[repetition].UpdateSQSMessageBody(invoke_index, sqs_message_body);
}

void BenchmarkResult::SetFunctionWarmUpCost(const size_t repetition, const long double cost) {
  benchmark_repetitions_[repetition].SetFunctionWarmUpCost(cost);
}

double BenchmarkResult::GetDurationMs() const {
  return std::accumulate(benchmark_repetitions_.cbegin(), benchmark_repetitions_.cend(), 0.0,
                         [](const double a, const BenchmarkRepetition& b) { return a + b.GetDurationMs(); });
}

const std::vector<BenchmarkRepetition>& BenchmarkResult::GetBenchmarkRepetitions() const {
  return benchmark_repetitions_;
}

long double BenchmarkResult::GetWarmUpCost() const {
  return std::accumulate(benchmark_repetitions_.cbegin(), benchmark_repetitions_.cend(), 0.0L,
                         [](const long double a, const BenchmarkRepetition& b) { return a + b.GetWarmUpCost(); });
}

bool BenchmarkResult::HasRepetitionFinished(const size_t repetition) const {
  return benchmark_repetitions_[repetition].IsComplete();
}

bool BenchmarkResult::IsComplete() const {
  return std::all_of(benchmark_repetitions_.cbegin(), benchmark_repetitions_.cend(),
                     [](const BenchmarkRepetition& benchmark_repetition) { return benchmark_repetition.IsComplete(); });
}

}  // namespace skyrise
