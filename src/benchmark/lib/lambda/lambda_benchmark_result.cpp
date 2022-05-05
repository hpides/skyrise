#include "lambda_benchmark_result.hpp"

#include <algorithm>
#include <numeric>

#include "utils/assert.hpp"
#include "utils/string.hpp"

namespace skyrise {

namespace {

bool IsSameType(const Aws::Utils::Json::JsonView& a, const Aws::Utils::Json::JsonView& b) {
  return (a.IsObject() && b.IsObject()) || (a.IsBool() && b.IsBool()) || (a.IsString() && b.IsString()) ||
         (a.IsIntegerType() && b.IsIntegerType()) || (a.IsFloatingPointType() && b.IsFloatingPointType()) ||
         (a.IsListType() && b.IsListType()) || (a.IsNull() && b.IsNull());
}

bool TraverseJsonNode(const Aws::Utils::Json::JsonView& response_node,
                      const Aws::Utils::Json::JsonView& template_node) {
  if (!IsSameType(response_node, template_node)) {
    return false;
  }

  if (response_node.IsObject()) {
    for (const auto& [key, value] : template_node.GetAllObjects()) {
      if (!response_node.KeyExists(key)) {
        return false;
      }

      if (!TraverseJsonNode(response_node.GetObject(key), value)) {
        return false;
      }
    }
  }

  return true;
}

}  // namespace

LambdaInvokeResult::LambdaInvokeResult(Aws::String invoke_id)
    : invoke_id_(std::move(invoke_id)),
      success_(false),
      complete_(false),
      start_point_(std::chrono::system_clock::now()),
      end_point_(start_point_) {}

void LambdaInvokeResult::Complete(Aws::Lambda::Model::InvokeOutcome* invoke_outcome) {
  end_point_ = std::chrono::system_clock::now();

  Assert(!complete_, "LambdaInvokeResult " + invoke_id_ + " is already complete.");
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

void LambdaInvokeResult::UpdateSQSMessageBody(const Aws::String& sqs_message_body) {
  Assert(complete_, "LambdaInvokeResult " + invoke_id_ + " must be complete before updating the SQS message.");
  const auto sqs_message_body_value = Aws::Utils::Json::JsonValue(sqs_message_body);

  const auto sqs_message_body_view = sqs_message_body_value.View();
  Assert(sqs_message_body_view.KeyExists("responsePayload"), "SQS message must contain a response payload.");

  response_body_ = sqs_message_body_view.GetObject("responsePayload").Materialize();
}

void LambdaInvokeResult::ValidateResponseBody(const Aws::Utils::Json::JsonView& expected_response_template) {
  const auto& response_body_view = response_body_.View();
  success_ = TraverseJsonNode(response_body_view, expected_response_template);
}

const Aws::String& LambdaInvokeResult::GetInvokeId() const { return invoke_id_; }

Aws::Utils::Json::JsonView LambdaInvokeResult::GetResponseBody() const { return response_body_.View(); }

std::shared_ptr<const LogResult> LambdaInvokeResult::GetLogResult() const { return log_result_; }

double LambdaInvokeResult::GetDurationMs() const {
  Assert(complete_, "LambdaInvokeResult " + invoke_id_ + " must be complete before a duration is available.");
  return std::chrono::duration<double, std::milli>(end_point_ - start_point_).count();
}

const std::chrono::time_point<std::chrono::system_clock>& LambdaInvokeResult::GetStartPoint() const {
  return start_point_;
}

const std::chrono::time_point<std::chrono::system_clock>& LambdaInvokeResult::GetEndPoint() const { return end_point_; }

bool LambdaInvokeResult::HasLogResult() const { return log_result_ != nullptr; }

bool LambdaInvokeResult::IsSuccess() const { return success_; }

bool LambdaInvokeResult::IsComplete() const { return complete_; }

LambdaBenchmarkRepetition::LambdaBenchmarkRepetition(const size_t invocation_count)
    : invocation_count_(invocation_count),
      invoke_results_(std::vector<LambdaInvokeResult>(invocation_count_)),
      warm_up_cost_(0) {}

void LambdaBenchmarkRepetition::RegisterInvocation(const size_t invoke_index, const Aws::String& invoke_id) {
  invoke_results_[invoke_index] = LambdaInvokeResult(invoke_id);
}

void LambdaBenchmarkRepetition::CompleteInvocation(const size_t invoke_index,
                                                   Aws::Lambda::Model::InvokeOutcome* invoke_outcome) {
  invoke_results_[invoke_index].Complete(invoke_outcome);
}

void LambdaBenchmarkRepetition::UpdateSQSMessageBody(const size_t invoke_index, const Aws::String& sqs_message_body) {
  invoke_results_[invoke_index].UpdateSQSMessageBody(sqs_message_body);
}

void LambdaBenchmarkRepetition::SetFunctionWarmUpCost(const long double cost) { warm_up_cost_ = cost; }

void LambdaBenchmarkRepetition::ValidateInvokeResults(const Aws::Utils::Json::JsonView& expected_response_template) {
  for (auto& invoke_result : invoke_results_) {
    invoke_result.ValidateResponseBody(expected_response_template);
  }
}

const std::vector<LambdaInvokeResult>& LambdaBenchmarkRepetition::GetInvokeResults() const { return invoke_results_; }

long double LambdaBenchmarkRepetition::GetWarmUpCost() const { return warm_up_cost_; }

double LambdaBenchmarkRepetition::GetDurationMs() const {
  Assert(IsComplete(), "LambdaBenchmarkRepetition must be completed before a duration is available.");

  const auto start_point_minimum = std::min_element(
      invoke_results_.cbegin(), invoke_results_.cend(),
      [](const LambdaInvokeResult& a, const LambdaInvokeResult& b) { return a.GetStartPoint() < b.GetStartPoint(); });

  const auto end_point_maximum = std::max_element(
      invoke_results_.cbegin(), invoke_results_.cend(),
      [](const LambdaInvokeResult& a, const LambdaInvokeResult& b) { return a.GetEndPoint() < b.GetEndPoint(); });

  return std::chrono::duration<double, std::milli>(end_point_maximum->GetEndPoint() -
                                                   start_point_minimum->GetStartPoint())
      .count();
}

bool LambdaBenchmarkRepetition::IsComplete() const {
  return std::all_of(invoke_results_.cbegin(), invoke_results_.cend(),
                     [](const LambdaInvokeResult& invoke_result) { return invoke_result.IsComplete(); });
}

LambdaBenchmarkResult::LambdaBenchmarkResult(const size_t repetition_count, const size_t invocation_count)
    : repetition_count_(repetition_count),
      invocation_count_(invocation_count),
      benchmark_repetitions_(repetition_count_, LambdaBenchmarkRepetition(invocation_count_)) {
  Assert(repetition_count > 0, "Repetition count must be greater than zero.");
  Assert(invocation_count > 0, "Invocation count must be greater than zero.");
}

void LambdaBenchmarkResult::RegisterInvocation(const size_t repetition, const size_t invoke_index,
                                               const Aws::String& invoke_id) {
  benchmark_repetitions_[repetition].RegisterInvocation(invoke_index, invoke_id);
}

void LambdaBenchmarkResult::FinishInvocation(const size_t repetition, const size_t invoke_index,
                                             Aws::Lambda::Model::InvokeOutcome* invoke_outcome) {
  benchmark_repetitions_[repetition].CompleteInvocation(invoke_index, invoke_outcome);
}
void LambdaBenchmarkResult::UpdateSQSMessageBody(const size_t repetition, const size_t invoke_index,
                                                 const Aws::String& sqs_message_body) {
  benchmark_repetitions_[repetition].UpdateSQSMessageBody(invoke_index, sqs_message_body);
}

void LambdaBenchmarkResult::SetFunctionWarmUpCost(const size_t repetition, const long double cost) {
  benchmark_repetitions_[repetition].SetFunctionWarmUpCost(cost);
}

void LambdaBenchmarkResult::ValidateInvokeResults(const Aws::Utils::Json::JsonView& expected_response_template) {
  for (auto& benchmark_repetition : benchmark_repetitions_) {
    benchmark_repetition.ValidateInvokeResults(expected_response_template);
  }
}

double LambdaBenchmarkResult::GetDurationMs() const {
  return std::accumulate(benchmark_repetitions_.cbegin(), benchmark_repetitions_.cend(), 0.0,
                         [](const double a, const LambdaBenchmarkRepetition& b) { return a + b.GetDurationMs(); });
}

const std::vector<LambdaBenchmarkRepetition>& LambdaBenchmarkResult::GetBenchmarkRepetitions() const {
  return benchmark_repetitions_;
}

long double LambdaBenchmarkResult::GetWarmUpCost() const {
  return std::accumulate(benchmark_repetitions_.cbegin(), benchmark_repetitions_.cend(), 0.0L,
                         [](const long double a, const LambdaBenchmarkRepetition& b) { return a + b.GetWarmUpCost(); });
}

bool LambdaBenchmarkResult::HasRepetitionFinished(const size_t repetition) const {
  return benchmark_repetitions_[repetition].IsComplete();
}

bool LambdaBenchmarkResult::IsResultComplete() const {
  return std::all_of(
      benchmark_repetitions_.cbegin(), benchmark_repetitions_.cend(),
      [](const LambdaBenchmarkRepetition& benchmark_repetition) { return benchmark_repetition.IsComplete(); });
}

}  // namespace skyrise
