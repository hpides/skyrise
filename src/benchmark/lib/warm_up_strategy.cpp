#include "warm_up_strategy.hpp"

#include <regex>

#include <aws/core/Aws.h>
#include <aws/core/utils/Outcome.h>
#include <aws/core/utils/base64/Base64.h>
#include <aws/core/utils/json/JsonSerializer.h>
#include <aws/lambda/model/InvokeRequest.h>

namespace skyrise {

WarmUpStrategy::WarmUpStrategy(const bool warm_up_once) : warm_up_once_(warm_up_once), was_warmed_up_(false) {}

SimpleWarmUpStrategy::SimpleWarmUpStrategy(const bool warm_up_once) : WarmUpStrategy(warm_up_once) {}

SleepWarmUpStrategy::SleepWarmUpStrategy(const bool warm_up_once) : WarmUpStrategy(warm_up_once) {}

long double WarmUpStrategy::WarmUpFunctions(const std::shared_ptr<Client>& client,
                                            const FunctionConfig& function_config, const size_t function_count) {
  if (warm_up_once_ && was_warmed_up_) {
    return 0.0;
  }

  std::vector<Aws::Lambda::Model::InvokeRequest> invoke_requests;
  invoke_requests.reserve(function_count);

  for (size_t i = 0; i < function_count; ++i) {
    invoke_requests.emplace_back(Aws::Lambda::Model::InvokeRequest()
                                     .WithFunctionName(function_config.function_name)
                                     .WithLogType(Aws::Lambda::Model::LogType::Tail));

    invoke_requests[i].SetBody(CreateInvokeRequestBody());
    invoke_requests[i].SetContentType("application/json");
  }

  std::vector<Aws::Lambda::Model::InvokeOutcomeCallable> invoke_outcome_callables;
  invoke_outcome_callables.reserve(function_count);

  for (const auto& invoke_request : invoke_requests) {
    invoke_outcome_callables.emplace_back(client->GetLambdaClient().InvokeCallable(invoke_request));
  }

  was_warmed_up_ = true;

  return CalculateWarmUpCost(client, function_config, &invoke_outcome_callables);
}

long double WarmUpStrategy::CalculateWarmUpCost(
    const std::shared_ptr<Client>& client, const FunctionConfig& function_config,
    std::vector<Aws::Lambda::Model::InvokeOutcomeCallable>* invoke_outcome_callables) {
  if (!cost_calculator_) {
    cost_calculator_ = std::make_unique<CostCalculator>(client);
  }

  long double function_warm_up_cost = 0;

  for (auto& outcome_callable : *invoke_outcome_callables) {
    const auto result = outcome_callable.get().GetResultWithOwnership();

    // TODO(anyone): Decouple Base64 decoding and LogResult parsing from WarmUpStrategy. The BenchmarkHelper class
    // provides this feature. However, it is tied to skyrise::InvocationResult. We need to refactor this out into a
    // separate helper class independent of the Benchmark context.
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

std::shared_ptr<Aws::IOStream> SimpleWarmUpStrategy::CreateInvokeRequestBody() const {
  const auto json_value = Aws::Utils::Json::JsonValue().WithBool("warmup", true);
  return std::make_shared<Aws::StringStream>(json_value.View().WriteCompact());
}

std::shared_ptr<Aws::IOStream> SleepWarmUpStrategy::CreateInvokeRequestBody() const {
  const auto json_value = Aws::Utils::Json::JsonValue().WithBool("warmup", true).WithInteger("sleep_ms", kSleepMs);
  return std::make_shared<Aws::StringStream>(json_value.View().WriteCompact());
}

std::string SimpleWarmUpStrategy::GetName() const { return "SimpleWarmUpStrategy"; }

std::string SleepWarmUpStrategy::GetName() const { return "SleepWarmUpStrategy"; }

}  // namespace skyrise
