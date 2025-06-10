#pragma once

#include <chrono>
#include <string>

#include <aws/lambda/LambdaClient.h>

#include "function/function_config.hpp"
#include "utils/costs/cost_calculator.hpp"

namespace skyrise {

// NOLINTNEXTLINE(cppcoreguidelines-special-member-functions,hicpp-special-member-functions)
class WarmupStrategy {
 public:
  virtual ~WarmupStrategy() = default;

  virtual long double WarmupFunctions(const std::shared_ptr<const Aws::Lambda::LambdaClient>& lambda_client,
                                      const std::shared_ptr<const CostCalculator>& cost_calculator,
                                      const FunctionConfig& function_config, const size_t concurrent_instance_count,
                                      const bool is_same_aws_region) = 0;
  virtual std::string Name() const = 0;
};

class ConfigurableWarmupStrategy : public WarmupStrategy {
 public:
  explicit ConfigurableWarmupStrategy(const bool warmup_once = kDefaultWarmupOnce,
                                      const double provisioning_factor = kDefaultWarmupProvisioningFactor);

  long double WarmupFunctions(const std::shared_ptr<const Aws::Lambda::LambdaClient>& lambda_client,
                              const std::shared_ptr<const CostCalculator>& cost_calculator,
                              const FunctionConfig& function_config, const size_t concurrent_instance_count,
                              const bool is_same_aws_region) override;
  std::string Name() const override;

  static long double CalculateWarmUpCost(
      const std::shared_ptr<const CostCalculator>& cost_calculator, const FunctionConfig& function_config,
      std::vector<Aws::Lambda::Model::InvokeOutcomeCallable>* invoke_outcome_callables);

  static constexpr bool kDefaultWarmupOnce = true;
  static constexpr double kDefaultWarmupProvisioningFactor = 1.2;

 private:
  bool is_warm_ = false;

  const bool warmup_once_;
  const double provisioning_factor_;
};

class ProvisionedConcurrencyWarmUpStrategy : public WarmupStrategy {
 public:
  long double WarmupFunctions(const std::shared_ptr<const Aws::Lambda::LambdaClient>& lambda_client,
                              const std::shared_ptr<const CostCalculator>& cost_calculator,
                              const FunctionConfig& function_config, const size_t concurrent_instance_count,
                              const bool is_same_aws_region) override;
  std::string Name() const override;

 private:
  std::chrono::time_point<std::chrono::steady_clock> provisioned_concurrency_started_;
  std::chrono::time_point<std::chrono::steady_clock> provisioned_concurrency_last_visited_;
};

}  // namespace skyrise
