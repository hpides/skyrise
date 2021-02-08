#include "warm_up_strategy.hpp"

#include <aws/core/Aws.h>
#include <aws/core/utils/Outcome.h>
#include <aws/core/utils/json/JsonSerializer.h>
#include <aws/lambda/model/InvokeRequest.h>

namespace skyrise {

WarmUpStrategy::WarmUpStrategy(const bool warm_up_once) : warm_up_once_(warm_up_once), was_warmed_up_(false) {}

SimpleWarmUpStrategy::SimpleWarmUpStrategy(const bool warm_up_once) : WarmUpStrategy(warm_up_once) {}

void SimpleWarmUpStrategy::WarmUpFunctions(const std::shared_ptr<Client>& client, const std::string& function_name,
                                           const size_t function_count) {
  if (warm_up_once_ && was_warmed_up_) {
    return;
  }

  std::vector<Aws::Lambda::Model::InvokeRequest> invoke_requests;
  invoke_requests.reserve(function_count);

  for (size_t i = 0; i < function_count; ++i) {
    invoke_requests.emplace_back(Aws::Lambda::Model::InvokeRequest().WithFunctionName(function_name));

    const auto json_value = Aws::Utils::Json::JsonValue().WithBool("warmup", true);
    const auto body = std::make_shared<Aws::StringStream>(json_value.View().WriteCompact());

    invoke_requests[i].SetBody(body);
    invoke_requests[i].SetContentType("application/json");
  }

  std::vector<Aws::Lambda::Model::InvokeOutcomeCallable> future_results;
  future_results.reserve(function_count);

  for (const auto& invoke_request : invoke_requests) {
    future_results.emplace_back(client->GetLambdaClient().InvokeCallable(invoke_request));
  }

  for (auto& result : future_results) {
    result.get();
  }

  was_warmed_up_ = true;
}

std::string SimpleWarmUpStrategy::GetName() const { return "SimpleWarmUpStrategy"; }

// TODO(anyone): Eliminate magic number once we understand the parallel running lambda functions better
const size_t kSleepMs = 7000;

SleepWarmUpStrategy::SleepWarmUpStrategy(const bool warm_up_once) : WarmUpStrategy(warm_up_once) {}

void SleepWarmUpStrategy::WarmUpFunctions(const std::shared_ptr<Client>& client, const std::string& function_name,
                                          const size_t function_count) {
  if (warm_up_once_ && was_warmed_up_) {
    return;
  }

  std::vector<Aws::Lambda::Model::InvokeRequest> invoke_requests;
  invoke_requests.reserve(function_count);

  for (size_t i = 0; i < function_count; ++i) {
    invoke_requests.emplace_back(Aws::Lambda::Model::InvokeRequest().WithFunctionName(function_name));

    const auto json_value = Aws::Utils::Json::JsonValue().WithBool("warmup", true).WithInteger("sleep_ms", kSleepMs);
    const auto body = std::make_shared<Aws::StringStream>(json_value.View().WriteCompact());

    invoke_requests[i].SetBody(body);
    invoke_requests[i].SetContentType("application/json");
  }

  std::vector<Aws::Lambda::Model::InvokeOutcomeCallable> future_results;
  future_results.reserve(function_count);

  for (const auto& invoke_request : invoke_requests) {
    future_results.emplace_back(client->GetLambdaClient().InvokeCallable(invoke_request));
  }

  for (auto& result : future_results) {
    result.get();
  }

  was_warmed_up_ = true;
}

std::string SleepWarmUpStrategy::GetName() const { return "SleepWarmUpStrategy"; }

}  // namespace skyrise