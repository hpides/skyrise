#pragma once

#include <chrono>
#include <map>
#include <memory>
#include <mutex>
#include <tuple>
#include <vector>

#include <aws/core/Aws.h>
#include <aws/lambda/LambdaClient.h>

#include "abstract_benchmark_result.hpp"
#include "lambda_result_log.hpp"
#include "utils/costs/cost_calculator.hpp"

namespace skyrise {

class LambdaInvocationResult {
 public:
  explicit LambdaInvocationResult(Aws::String instance_id = {});

  const Aws::String& GetInstanceId() const;
  Aws::String GetEnvironmentId() const;

  const std::chrono::time_point<std::chrono::system_clock>& GetStartPoint() const;
  const std::chrono::time_point<std::chrono::system_clock>& GetEndPoint() const;
  double GetDurationMs() const;
  double GetDurationSeconds() const;
  size_t GetSleepDurationSeconds() const;

  void Complete(Aws::Lambda::Model::InvokeOutcome* invocation_outcome);
  void UpdateSQSMessageBody(const Aws::String& sqs_message_body);

  bool IsComplete() const;
  bool IsSuccess() const;

  Aws::Utils::Json::JsonView GetResponseBody() const;
  void ValidateResponseBody(const Aws::Utils::Json::JsonView& expected_response_template);

  bool HasResultLog() const;
  std::shared_ptr<const LambdaResultLog> GetResultLog() const;

  long double CalculateCost(const std::shared_ptr<const CostCalculator>& cost_calculator,
                            const size_t function_instance_size_mb,
                            const bool is_provisioned_concurrency = false) const;

 private:
  Aws::String instance_id_;
  std::chrono::time_point<std::chrono::system_clock> start_point_;
  std::chrono::time_point<std::chrono::system_clock> end_point_;
  bool complete_{false};
  bool success_{false};
  Aws::Utils::Json::JsonValue response_body_;
  std::shared_ptr<const LambdaResultLog> result_log_;
};

class LambdaBenchmarkRepetitionResult {
 public:
  explicit LambdaBenchmarkRepetitionResult(const size_t concurrent_invocation_count);

  void RegisterInvocation(const size_t invocation_index, const Aws::String& instance_id);
  void CompleteInvocation(const size_t invocation_index, Aws::Lambda::Model::InvokeOutcome* invocation_outcome);

  std::string GetEndTimestamp() const;
  double GetDurationMs() const;
  double GetDurationSeconds() const;
  double GetAccumulatedLambdaDurationMs() const;

  void UpdateSQSMessageBody(const size_t invocation_index, const Aws::String& sqs_message_body);

  bool IsComplete() const;

  const std::vector<LambdaInvocationResult>& GetInvocationResults() const;
  void ValidateInvocationResults(const Aws::Utils::Json::JsonView& expected_response_template);

  void SetWarmupCost(const long double cost);
  long double GetWarmupCost() const;
  long double CalculateRuntimeCost(const std::shared_ptr<const CostCalculator>& cost_calculator,
                                   const size_t function_instance_size_mb,
                                   const bool is_provisioned_concurrency = false) const;

 private:
  std::chrono::time_point<std::chrono::system_clock> start_point_ =
      std::chrono::time_point<std::chrono::system_clock>::max();
  std::chrono::time_point<std::chrono::system_clock> end_point_;
  std::string end_timestamp_;
  const size_t concurrent_invocation_count_;
  std::vector<LambdaInvocationResult> invocation_results_;
  long double warmup_cost_usd_{0.0};
};

class LambdaBenchmarkResult : public AbstractBenchmarkResult {
 public:
  LambdaBenchmarkResult(const size_t concurrent_invocation_count, const size_t repetition_count,
                        const std::shared_ptr<const CostCalculator>& cost_calculator);

  void RegisterInvocation(const size_t repetition, const size_t invocation_index, const Aws::String& instance_id);
  void CompleteInvocation(const size_t repetition, const size_t invocation_index,
                          Aws::Lambda::Model::InvokeOutcome* invocation_outcome);

  void UpdateSQSMessageBody(const size_t repetition, const size_t invocation_index,
                            const Aws::String& sqs_message_body);

  bool HasRepetitionFinished(const size_t repetition) const;
  bool IsResultComplete() const override;
  void ValidateInvocationResults(const Aws::Utils::Json::JsonView& expected_response_template);
  const std::vector<LambdaBenchmarkRepetitionResult>& GetRepetitionResults() const;

  void SetEndTime();
  double GetAccumulatedLambdaDurationMs() const;
  double GetDurationMs() const override;

  std::shared_ptr<const CostCalculator> GetCostCalculator() const;
  long double CalculateWarmupCost() const;
  void SetRepetitionWarmupCost(const size_t repetition, const long double cost);
  long double CalculateRuntimeCost(const size_t function_instance_size_mb,
                                   const bool is_provisioned_concurrency = false) const;
  void SetTraceCost(const long double cost);
  long double GetTraceCost() const;

 private:
  std::chrono::time_point<std::chrono::system_clock> start_point_;
  std::chrono::time_point<std::chrono::system_clock> end_point_;
  const size_t concurrent_invocation_count_;
  const size_t repetition_count_;
  std::vector<LambdaBenchmarkRepetitionResult> repetition_results_;
  const std::shared_ptr<const CostCalculator> cost_calculator_;
  long double trace_cost_usd_{0.0};
};

}  // namespace skyrise
