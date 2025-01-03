#include "warmup_strategy.hpp"

#include <cmath>
#include <regex>
#include <thread>

#include <aws/core/Aws.h>
#include <aws/core/utils/Outcome.h>
#include <aws/core/utils/base64/Base64.h>
#include <aws/core/utils/json/JsonSerializer.h>
#include <aws/core/utils/logging/LogMacros.h>
#include <aws/lambda/model/GetProvisionedConcurrencyConfigRequest.h>
#include <aws/lambda/model/InvokeRequest.h>
#include <aws/lambda/model/PutProvisionedConcurrencyConfigRequest.h>
#include <magic_enum/magic_enum.hpp>

#include "constants.hpp"
#include "utils/assert.hpp"

namespace skyrise {

ConfigurableWarmupStrategy::ConfigurableWarmupStrategy(const bool warmup_once, const double provisioning_factor)
    : warmup_once_(warmup_once), provisioning_factor_(provisioning_factor) {}

long double ConfigurableWarmupStrategy::WarmupFunctions(
    const std::shared_ptr<const Aws::Lambda::LambdaClient>& lambda_client,
    const std::shared_ptr<const CostCalculator>& cost_calculator, const FunctionConfig& function_config,
    const size_t concurrent_instance_count, const bool is_same_aws_region) {
  if (warmup_once_ && is_warm_) {
    return 0.0L;
  }

  const auto provisioned_instance_count = static_cast<size_t>(concurrent_instance_count * provisioning_factor_);
  const size_t concurrency_duration_seconds =
      std::ceil(is_same_aws_region ? std::cbrt(provisioned_instance_count) : std::sqrt(provisioned_instance_count));

  std::vector<Aws::Lambda::Model::InvokeRequest> invoke_requests;
  invoke_requests.reserve(provisioned_instance_count);

  for (size_t i = 0; i < provisioned_instance_count; ++i) {
    const auto request_body = Aws::Utils::Json::JsonValue()
                                  .WithBool("warmup", true)
                                  .WithInteger("concurrency_duration_seconds", concurrency_duration_seconds);
    const auto warmup_request_body = std::make_shared<Aws::StringStream>(request_body.View().WriteCompact());

    invoke_requests.emplace_back(Aws::Lambda::Model::InvokeRequest()
                                     .WithFunctionName(function_config.name)
                                     .WithQualifier("1")
                                     .WithLogType(Aws::Lambda::Model::LogType::Tail));
    invoke_requests[i].SetBody(warmup_request_body);
    invoke_requests[i].SetContentType("application/json");
  }

  std::vector<Aws::Lambda::Model::InvokeOutcomeCallable> invoke_outcome_callables;
  invoke_outcome_callables.reserve(concurrent_instance_count);

  for (const auto& invoke_request : invoke_requests) {
    invoke_outcome_callables.emplace_back(lambda_client->InvokeCallable(invoke_request));
  }

  is_warm_ = true;

  return CalculateWarmUpCost(cost_calculator, function_config, &invoke_outcome_callables);
}

std::string ConfigurableWarmupStrategy::Name() const { return "ConfigurableWarmUpStrategy"; }

long double ConfigurableWarmupStrategy::CalculateWarmUpCost(
    const std::shared_ptr<const CostCalculator>& cost_calculator, const FunctionConfig& function_config,
    std::vector<Aws::Lambda::Model::InvokeOutcomeCallable>* invoke_outcome_callables) {
  long double function_warmup_cost = 0;

  for (auto& outcome_callable : *invoke_outcome_callables) {
    auto outcome = outcome_callable.get();

    if (outcome.IsSuccess()) {
      const auto result = outcome.GetResultWithOwnership();

      // TODO(tobodner): Decouple Base64 decoding and LogResult parsing from WarmUpStrategy
      const Aws::Utils::ByteBuffer log_result_chars = Aws::Utils::Base64::Base64().Decode(result.GetLogResult());
      const std::string log_result(reinterpret_cast<char const*>(log_result_chars.GetUnderlyingData()),
                                   log_result_chars.GetLength());

      const std::regex metric_regex("REPORT.+Billed Duration: ([\\d\\.]+)");
      std::smatch metric_match;
      std::regex_search(log_result, metric_match, metric_regex);

      function_warmup_cost +=
          cost_calculator->CalculateCostLambda(std::stod(metric_match[1]), function_config.memory_size);
    }
  }

  return function_warmup_cost;
}

long double ProvisionedConcurrencyWarmUpStrategy::WarmupFunctions(
    const std::shared_ptr<const Aws::Lambda::LambdaClient>& lambda_client,
    const std::shared_ptr<const CostCalculator>& cost_calculator, const FunctionConfig& function_config,
    const size_t concurrent_instance_count, const bool /* is_same_aws_region */) {
  const auto get_config_outcome =
      lambda_client->GetProvisionedConcurrencyConfig(Aws::Lambda::Model::GetProvisionedConcurrencyConfigRequest()
                                                         .WithFunctionName(function_config.name)
                                                         .WithQualifier("1"));
  if (get_config_outcome.IsSuccess() &&
      get_config_outcome.GetResult().GetStatus() == Aws::Lambda::Model::ProvisionedConcurrencyStatusEnum::READY) {
    const auto now = std::chrono::steady_clock::now();
    const size_t duration_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(now - provisioned_concurrency_last_visited_).count();
    provisioned_concurrency_last_visited_ = now;

    return cost_calculator->CalculateCostLambdaProvisionedConcurrency(duration_ms, function_config.memory_size,
                                                                      concurrent_instance_count);
  } else {
    provisioned_concurrency_started_ = std::chrono::steady_clock::now();
    const auto put_config_outcome = lambda_client->PutProvisionedConcurrencyConfig(
        Aws::Lambda::Model::PutProvisionedConcurrencyConfigRequest()
            .WithFunctionName(function_config.name)
            .WithProvisionedConcurrentExecutions(concurrent_instance_count)
            .WithQualifier("1"));
    Assert(put_config_outcome.IsSuccess(), put_config_outcome.GetError().GetMessage());

    auto status = Aws::Lambda::Model::ProvisionedConcurrencyStatusEnum::NOT_SET;

    while (status != Aws::Lambda::Model::ProvisionedConcurrencyStatusEnum::READY) {
      std::this_thread::sleep_for(std::chrono::seconds(10));
      const auto get_config_outcome =
          lambda_client->GetProvisionedConcurrencyConfig(Aws::Lambda::Model::GetProvisionedConcurrencyConfigRequest()
                                                             .WithFunctionName(function_config.name)
                                                             .WithQualifier("1"));

      Assert(get_config_outcome.IsSuccess(), get_config_outcome.GetError().GetMessage());
      const auto& config_result = get_config_outcome.GetResult();

      status = config_result.GetStatus();
      Assert(status != Aws::Lambda::Model::ProvisionedConcurrencyStatusEnum::FAILED, config_result.GetStatusReason());

      AWS_LOGSTREAM_INFO(kCoordinatorTag.c_str(),
                         config_result.GetAllocatedProvisionedConcurrentExecutions()
                             << "/" << concurrent_instance_count
                             << " function instances warmed up with Provisioned Concurrency. Status: "
                             << magic_enum::enum_name(status));
    }

    provisioned_concurrency_last_visited_ = std::chrono::steady_clock::now();
    const size_t duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                   provisioned_concurrency_last_visited_ - provisioned_concurrency_started_)
                                   .count();

    return cost_calculator->CalculateCostLambdaProvisionedConcurrency(duration_ms, function_config.memory_size,
                                                                      concurrent_instance_count);
  }
}

std::string ProvisionedConcurrencyWarmUpStrategy::Name() const { return "ProvisionedConcurrencyWarmUpStrategy"; }

}  // namespace skyrise
