#include "lambda_benchmark_result.hpp"

#include <algorithm>
#include <chrono>
#include <numeric>

#include "utils/assert.hpp"
#include "utils/string.hpp"
#include "utils/time.hpp"

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

LambdaInvocationResult::LambdaInvocationResult(Aws::String instance_id)
    : instance_id_(std::move(instance_id)),
      start_point_(std::chrono::system_clock::now()),
      end_point_(start_point_),
      result_log_(nullptr) {}

const Aws::String& LambdaInvocationResult::GetInstanceId() const { return instance_id_; }

Aws::String LambdaInvocationResult::GetEnvironmentId() const {
  const auto response_body = GetResponseBody();
  return response_body.KeyExists("environment_id") ? response_body.GetString("environment_id") : "";
}

const std::chrono::time_point<std::chrono::system_clock>& LambdaInvocationResult::GetStartPoint() const {
  return start_point_;
}

const std::chrono::time_point<std::chrono::system_clock>& LambdaInvocationResult::GetEndPoint() const {
  return end_point_;
}

double LambdaInvocationResult::GetDurationMs() const {
  Assert(complete_, "LambdaInvocationResult " + instance_id_ + " must be complete before a duration is available.");
  return std::chrono::duration<double, std::milli>(end_point_ - start_point_).count();
}

double LambdaInvocationResult::GetDurationSeconds() const {
  return std::chrono::duration<double>(std::chrono::duration<double, std::milli>(GetDurationMs())).count();
}

size_t LambdaInvocationResult::GetSleepDurationSeconds() const {
  const auto response_body = GetResponseBody();
  return response_body.KeyExists("sleep_duration_seconds") ? response_body.GetInteger("sleep_duration_seconds") : 0;
}

void LambdaInvocationResult::Complete(Aws::Lambda::Model::InvokeOutcome* invocation_outcome) {
  end_point_ = std::chrono::system_clock::now();

  Assert(!complete_, "LambdaInvocationResult " + instance_id_ + " is already complete.");
  complete_ = true;
  success_ = invocation_outcome != nullptr && invocation_outcome->IsSuccess();

  if (success_) {
    auto invoke_result = invocation_outcome->GetResultWithOwnership();
    response_body_ = Aws::Utils::Json::JsonValue(StreamToString(&invoke_result.GetPayload()));

    if (!invoke_result.GetLogResult().empty()) {
      result_log_ = std::make_shared<const LambdaResultLog>(invoke_result.GetLogResult());
    }
  }
}

void LambdaInvocationResult::UpdateSQSMessageBody(const Aws::String& sqs_message_body) {
  Assert(complete_, "LambdaInvocationResult " + instance_id_ + " must be complete before updating the SQS message.");
  const auto sqs_message_body_value = Aws::Utils::Json::JsonValue(sqs_message_body);

  const auto sqs_message_body_view = sqs_message_body_value.View();
  Assert(sqs_message_body_view.KeyExists("responsePayload"), "SQS message must contain a response payload.");

  response_body_ = sqs_message_body_view.GetObject("responsePayload").Materialize();
}

bool LambdaInvocationResult::IsComplete() const { return complete_; }

bool LambdaInvocationResult::IsSuccess() const { return success_; }

Aws::Utils::Json::JsonView LambdaInvocationResult::GetResponseBody() const { return response_body_.View(); }

void LambdaInvocationResult::ValidateResponseBody(const Aws::Utils::Json::JsonView& expected_response_template) {
  const auto& response_body_view = response_body_.View();
  success_ = TraverseJsonNode(response_body_view, expected_response_template);
}

bool LambdaInvocationResult::HasResultLog() const { return result_log_ != nullptr; }

std::shared_ptr<const LambdaResultLog> LambdaInvocationResult::GetResultLog() const { return result_log_; }

long double LambdaInvocationResult::CalculateCost(const std::shared_ptr<const CostCalculator>& cost_calculator,
                                                  const size_t function_instance_size_mb,
                                                  const bool is_provisioned_concurrency) const {
  const double billed_duration_ms = HasResultLog() ? GetResultLog()->GetBilledDurationMs() : 0.0;
  const long double lambda_cost =
      cost_calculator->CalculateCostLambda(billed_duration_ms, function_instance_size_mb, is_provisioned_concurrency);

  const auto response_body = GetResponseBody();

  const size_t s3_requests_tier1_count =
      response_body.KeyExists("s3_requests_tier1_count") ? response_body.GetInteger("s3_requests_tier1_count") : 0;
  const size_t s3_requests_tier2_count =
      response_body.KeyExists("s3_requests_tier2_count") ? response_body.GetInteger("s3_requests_tier2_count") : 0;
  const long double s3_request_cost =
      cost_calculator->CalculateCostS3Requests(s3_requests_tier1_count, s3_requests_tier2_count);

  const size_t s3_used_storage_bytes =
      response_body.KeyExists("s3_used_storage_bytes") ? response_body.GetInt64("s3_used_storage_bytes") : 0;
  // The S3 objects generated by this benchmark are deleted within an hour.
  const long double s3_storage_cost = cost_calculator->CalculateCostS3StorageMonthly(s3_used_storage_bytes, 1);

  return lambda_cost + s3_request_cost + s3_storage_cost;
}

LambdaBenchmarkRepetitionResult::LambdaBenchmarkRepetitionResult(const size_t concurrent_invocation_count)
    : concurrent_invocation_count_(concurrent_invocation_count),
      invocation_results_(std::vector<LambdaInvocationResult>(concurrent_invocation_count_)) {}

void LambdaBenchmarkRepetitionResult::RegisterInvocation(const size_t invocation_index,
                                                         const Aws::String& instance_id) {
  if (start_point_ == std::chrono::time_point<std::chrono::system_clock>::max()) {
    start_point_ = std::chrono::system_clock::now();
  }

  invocation_results_[invocation_index] = LambdaInvocationResult(instance_id);
}

void LambdaBenchmarkRepetitionResult::CompleteInvocation(const size_t invocation_index,
                                                         Aws::Lambda::Model::InvokeOutcome* invocation_outcome) {
  invocation_results_[invocation_index].Complete(invocation_outcome);
  if (IsComplete()) {
    end_point_ = std::chrono::system_clock::now();
    end_timestamp_ = GetFormattedTimestamp("%Y%m%dT%H%M%S");
  }
}

std::string LambdaBenchmarkRepetitionResult::GetEndTimestamp() const { return end_timestamp_; }

double LambdaBenchmarkRepetitionResult::GetDurationMs() const {
  Assert(IsComplete(), "LambdaBenchmarkRepetitionResult must be completed before a duration is available.");
  return std::chrono::duration<double, std::milli>(end_point_ - start_point_).count();
}

double LambdaBenchmarkRepetitionResult::GetAccumulatedLambdaDurationMs() const {
  Assert(IsComplete(), "LambdaBenchmarkRepetitionResult must be completed before a duration is available.");
  return std::accumulate(invocation_results_.cbegin(), invocation_results_.cend(), 0.0,
                         [](const double a, const LambdaInvocationResult& b) { return a + b.GetDurationMs(); });
}

double LambdaBenchmarkRepetitionResult::GetDurationSeconds() const {
  return std::chrono::duration<double>(std::chrono::duration<double, std::milli>(GetDurationMs())).count();
}

void LambdaBenchmarkRepetitionResult::UpdateSQSMessageBody(const size_t invocation_index,
                                                           const Aws::String& sqs_message_body) {
  invocation_results_[invocation_index].UpdateSQSMessageBody(sqs_message_body);
}

bool LambdaBenchmarkRepetitionResult::IsComplete() const {
  return std::all_of(invocation_results_.cbegin(), invocation_results_.cend(),
                     [](const LambdaInvocationResult& invoke_result) { return invoke_result.IsComplete(); });
}

const std::vector<LambdaInvocationResult>& LambdaBenchmarkRepetitionResult::GetInvocationResults() const {
  return invocation_results_;
}

void LambdaBenchmarkRepetitionResult::ValidateInvocationResults(
    const Aws::Utils::Json::JsonView& expected_response_template) {
  for (auto& invoke_result : invocation_results_) {
    invoke_result.ValidateResponseBody(expected_response_template);
  }
}

void LambdaBenchmarkRepetitionResult::SetWarmupCost(const long double cost) { warmup_cost_usd_ = cost; }

long double LambdaBenchmarkRepetitionResult::GetWarmupCost() const { return warmup_cost_usd_; }

long double LambdaBenchmarkRepetitionResult::CalculateRuntimeCost(
    const std::shared_ptr<const CostCalculator>& cost_calculator, const size_t function_instance_size_mb,
    const bool is_provisioned_concurrency) const {
  const auto& lambda_invocation_results = GetInvocationResults();
  std::vector<long double> lambda_invocation_costs;
  lambda_invocation_costs.reserve(lambda_invocation_results.size());

  std::transform(lambda_invocation_results.cbegin(), lambda_invocation_results.cend(),
                 std::back_inserter(lambda_invocation_costs),
                 [&](const LambdaInvocationResult& lambda_invocation_result) {
                   return lambda_invocation_result.CalculateCost(cost_calculator, function_instance_size_mb,
                                                                 is_provisioned_concurrency);
                 });

  return std::accumulate(lambda_invocation_costs.cbegin(), lambda_invocation_costs.cend(), 0.0L);
}

LambdaBenchmarkResult::LambdaBenchmarkResult(const size_t concurrent_invocation_count, const size_t repetition_count,
                                             const std::shared_ptr<const CostCalculator>& cost_calculator)
    : start_point_(std::chrono::system_clock::now()),
      end_point_(start_point_),
      concurrent_invocation_count_(concurrent_invocation_count),
      repetition_count_(repetition_count),
      repetition_results_(repetition_count_, LambdaBenchmarkRepetitionResult(concurrent_invocation_count_)),
      cost_calculator_(cost_calculator) {
  Assert(concurrent_invocation_count > 0, "Concurrent invocation count must be greater than zero.");
  Assert(repetition_count > 0, "Repetition count must be greater than zero.");
}

void LambdaBenchmarkResult::RegisterInvocation(const size_t repetition, const size_t invocation_index,
                                               const Aws::String& instance_id) {
  repetition_results_[repetition].RegisterInvocation(invocation_index, instance_id);
}

void LambdaBenchmarkResult::CompleteInvocation(const size_t repetition, const size_t invocation_index,
                                               Aws::Lambda::Model::InvokeOutcome* invocation_outcome) {
  repetition_results_[repetition].CompleteInvocation(invocation_index, invocation_outcome);
}

void LambdaBenchmarkResult::UpdateSQSMessageBody(const size_t repetition, const size_t invocation_index,
                                                 const Aws::String& sqs_message_body) {
  repetition_results_[repetition].UpdateSQSMessageBody(invocation_index, sqs_message_body);
}

bool LambdaBenchmarkResult::HasRepetitionFinished(const size_t repetition) const {
  return repetition_results_[repetition].IsComplete();
}

bool LambdaBenchmarkResult::IsResultComplete() const {
  return std::all_of(
      repetition_results_.cbegin(), repetition_results_.cend(),
      [](const LambdaBenchmarkRepetitionResult& benchmark_repetition) { return benchmark_repetition.IsComplete(); });
}

void LambdaBenchmarkResult::ValidateInvocationResults(const Aws::Utils::Json::JsonView& expected_response_template) {
  for (auto& benchmark_repetition : repetition_results_) {
    benchmark_repetition.ValidateInvocationResults(expected_response_template);
  }
}

const std::vector<LambdaBenchmarkRepetitionResult>& LambdaBenchmarkResult::GetRepetitionResults() const {
  return repetition_results_;
}

double LambdaBenchmarkResult::GetAccumulatedLambdaDurationMs() const {
  return std::accumulate(
      repetition_results_.cbegin(), repetition_results_.cend(), 0.0,
      [](const double a, const LambdaBenchmarkRepetitionResult& b) { return a + b.GetAccumulatedLambdaDurationMs(); });
}

double LambdaBenchmarkResult::GetDurationMs() const {
  return std::chrono::duration<double, std::milli>(end_point_ - start_point_).count();
}

std::shared_ptr<const CostCalculator> LambdaBenchmarkResult::GetCostCalculator() const { return cost_calculator_; }

long double LambdaBenchmarkResult::CalculateWarmupCost() const {
  return std::accumulate(
      repetition_results_.cbegin(), repetition_results_.cend(), 0.0L,
      [](const long double a, const LambdaBenchmarkRepetitionResult& b) { return a + b.GetWarmupCost(); });
}

void LambdaBenchmarkResult::SetRepetitionWarmupCost(const size_t repetition, const long double cost) {
  repetition_results_[repetition].SetWarmupCost(cost);
}

long double LambdaBenchmarkResult::CalculateRuntimeCost(const size_t function_instance_size_mb,
                                                        const bool is_provisioned_concurrency) const {
  const auto& repetition_results = GetRepetitionResults();
  std::vector<long double> repetition_costs;
  repetition_costs.reserve(repetition_results.size());

  std::transform(repetition_results.cbegin(), repetition_results.cend(), std::back_inserter(repetition_costs),
                 [&](const LambdaBenchmarkRepetitionResult& repetition_result) {
                   return repetition_result.CalculateRuntimeCost(cost_calculator_, function_instance_size_mb,
                                                                 is_provisioned_concurrency);
                 });

  return std::accumulate(repetition_costs.cbegin(), repetition_costs.cend(), 0.0L);
}

void LambdaBenchmarkResult::SetTraceCost(const long double cost) { trace_cost_usd_ = cost; }

long double LambdaBenchmarkResult::GetTraceCost() const { return trace_cost_usd_; }

void LambdaBenchmarkResult::SetEndTime() { end_point_ = std::chrono::system_clock::now(); }

}  // namespace skyrise
