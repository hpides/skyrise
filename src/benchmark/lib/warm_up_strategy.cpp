#include "warm_up_strategy.hpp"

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
#include <magic_enum.hpp>

#include "utils/assert.hpp"

namespace skyrise {

const std::string kTag = "SKYRISE/BENCHMARK/WARM_UP_STRATEGY";

ConfigurableWarmUpStrategy::ConfigurableWarmUpStrategy(const bool warm_up_once, const size_t sleep_ms_duration,
                                                       const double provisioning_factor)
    : warm_up_once_(warm_up_once), sleep_ms_duration_(sleep_ms_duration), provisioning_factor_(provisioning_factor) {}

long double ConfigurableWarmUpStrategy::WarmUpFunctions(const std::shared_ptr<Client>& client,
                                                        const FunctionConfig& function_config,
                                                        const size_t concurrency_count) {
  if (warm_up_once_ && is_warmed_up_) {
    return 0.0L;
  }

  const auto provisioned_concurrency_count = static_cast<size_t>(concurrency_count * provisioning_factor_);

  std::vector<Aws::Lambda::Model::InvokeRequest> invoke_requests;
  invoke_requests.reserve(provisioned_concurrency_count);

  for (size_t i = 0; i < provisioned_concurrency_count; ++i) {
    invoke_requests.emplace_back(Aws::Lambda::Model::InvokeRequest()
                                     .WithFunctionName(function_config.function_name)
                                     .WithQualifier("1")
                                     .WithLogType(Aws::Lambda::Model::LogType::Tail));

    const auto json_value =
        Aws::Utils::Json::JsonValue().WithBool("warmup", true).WithInteger("sleep_ms", sleep_ms_duration_);

    invoke_requests[i].SetBody(std::make_shared<Aws::StringStream>(json_value.View().WriteCompact()));
    invoke_requests[i].SetContentType("application/json");
  }

  std::vector<Aws::Lambda::Model::InvokeOutcomeCallable> invoke_outcome_callables;
  invoke_outcome_callables.reserve(concurrency_count);

  for (const auto& invoke_request : invoke_requests) {
    invoke_outcome_callables.emplace_back(client->GetLambdaClient().InvokeCallable(invoke_request));
  }

  is_warmed_up_ = true;

  return CalculateWarmUpCost(client, function_config, &invoke_outcome_callables);
}

std::string ConfigurableWarmUpStrategy::GetName() const { return "ConfigurableWarmUpStrategy"; }

long double ConfigurableWarmUpStrategy::CalculateWarmUpCost(
    const std::shared_ptr<Client>& client, const FunctionConfig& function_config,
    std::vector<Aws::Lambda::Model::InvokeOutcomeCallable>* invoke_outcome_callables) {
  if (!cost_calculator_) {
    cost_calculator_ = std::make_unique<CostCalculator>(client);
  }

  long double function_warm_up_cost = 0;

  for (auto& outcome_callable : *invoke_outcome_callables) {
    const auto result = outcome_callable.get().GetResultWithOwnership();

    // TODO(anyone): Decouple Base64 decoding and LogResult parsing from WarmUpStrategy
    const Aws::Utils::ByteBuffer log_result_chars = Aws::Utils::Base64::Base64().Decode(result.GetLogResult());
    const std::string log_result(reinterpret_cast<char const*>(log_result_chars.GetUnderlyingData()),
                                 log_result_chars.GetLength());

    const std::regex metric_regex("REPORT.+Billed Duration: ([\\d\\.]+)");
    std::smatch metric_match;
    std::regex_search(log_result, metric_match, metric_regex);

    function_warm_up_cost +=
        cost_calculator_->CalculateCostLambda(std::stod(metric_match[1]), function_config.memory_size);
  }

  return function_warm_up_cost;
}

long double ProvisionedConcurrencyWarmUpStrategy::WarmUpFunctions(const std::shared_ptr<Client>& client,
                                                                  const FunctionConfig& function_config,
                                                                  const size_t concurrency_count) {
  const auto& lambda_client = client->GetLambdaClient();

  const auto get_config_outcome =
      lambda_client.GetProvisionedConcurrencyConfig(Aws::Lambda::Model::GetProvisionedConcurrencyConfigRequest()
                                                        .WithFunctionName(function_config.function_name)
                                                        .WithQualifier("1"));
  if (get_config_outcome.IsSuccess() &&
      get_config_outcome.GetResult().GetStatus() == Aws::Lambda::Model::ProvisionedConcurrencyStatusEnum::READY) {
    const auto now = std::chrono::steady_clock::now();
    const size_t duration_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(now - provisioned_concurrency_last_visited_).count();
    provisioned_concurrency_last_visited_ = now;

    return cost_calculator_->CalculateCostLambdaProvisionedConcurrency(duration_ms, function_config.memory_size,
                                                                       concurrency_count);
  } else {
    provisioned_concurrency_started_ = std::chrono::steady_clock::now();
    const auto put_config_outcome =
        lambda_client.PutProvisionedConcurrencyConfig(Aws::Lambda::Model::PutProvisionedConcurrencyConfigRequest()
                                                          .WithFunctionName(function_config.function_name)
                                                          .WithProvisionedConcurrentExecutions(concurrency_count)
                                                          .WithQualifier("1"));
    Assert(put_config_outcome.IsSuccess(), put_config_outcome.GetError().GetMessage());

    auto status = Aws::Lambda::Model::ProvisionedConcurrencyStatusEnum::NOT_SET;

    while (status != Aws::Lambda::Model::ProvisionedConcurrencyStatusEnum::READY) {
      std::this_thread::sleep_for(std::chrono::seconds(10));
      const auto get_config_outcome =
          lambda_client.GetProvisionedConcurrencyConfig(Aws::Lambda::Model::GetProvisionedConcurrencyConfigRequest()
                                                            .WithFunctionName(function_config.function_name)
                                                            .WithQualifier("1"));

      Assert(get_config_outcome.IsSuccess(), get_config_outcome.GetError().GetMessage());
      const auto& config_result = get_config_outcome.GetResult();

      status = config_result.GetStatus();
      Assert(status != Aws::Lambda::Model::ProvisionedConcurrencyStatusEnum::FAILED, config_result.GetStatusReason());

      AWS_LOGSTREAM_INFO(kTag.c_str(), config_result.GetAllocatedProvisionedConcurrentExecutions()
                                           << "/" << concurrency_count
                                           << " function instances warmed up with Provisioned Concurrency. Status: "
                                           << magic_enum::enum_name(status));
    }

    provisioned_concurrency_last_visited_ = std::chrono::steady_clock::now();
    const size_t duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                   provisioned_concurrency_last_visited_ - provisioned_concurrency_started_)
                                   .count();

    cost_calculator_ = std::make_unique<CostCalculator>(client);
    return cost_calculator_->CalculateCostLambdaProvisionedConcurrency(duration_ms, function_config.memory_size,
                                                                       concurrency_count);
  }
}

std::string ProvisionedConcurrencyWarmUpStrategy::GetName() const { return "ProvisionedConcurrencyWarmUpStrategy"; }

}  // namespace skyrise
