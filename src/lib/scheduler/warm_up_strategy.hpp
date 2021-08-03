#pragma once

#include <chrono>
#include <string>

#include "client/client.hpp"
#include "function_config.hpp"
#include "utils/costs/cost_calculator.hpp"

namespace skyrise {

// TODO(anyone): Move the best strategy to src/lib/scheduler
class WarmUpStrategy {
 public:
  virtual ~WarmUpStrategy() {}

  virtual long double WarmUpFunctions(const std::shared_ptr<const Aws::Lambda::LambdaClient>& lambda_client,
                                      const std::shared_ptr<const CostCalculator>& cost_calculator,
                                      const FunctionConfig& function_config, const size_t concurrency_count) = 0;
  virtual std::string GetName() const = 0;
};

class ConfigurableWarmUpStrategy : public WarmUpStrategy {
 public:
  explicit ConfigurableWarmUpStrategy(const bool warm_up_once = kDefaultWarmUpOnce,
                                      const size_t sleep_ms_duration = kDefaultSleepMsDuration,
                                      const double provisioning_factor = kDefaultProvisioningFactor);

  long double WarmUpFunctions(const std::shared_ptr<const Aws::Lambda::LambdaClient>& lambda_client,
                              const std::shared_ptr<const CostCalculator>& cost_calculator,
                              const FunctionConfig& function_config, const size_t concurrency_count) override;
  std::string GetName() const override;

  static long double CalculateWarmUpCost(
      const std::shared_ptr<const CostCalculator>& cost_calculator, const FunctionConfig& function_config,
      std::vector<Aws::Lambda::Model::InvokeOutcomeCallable>* invoke_outcome_callables);

  static constexpr bool kDefaultWarmUpOnce = true;
  static constexpr size_t kDefaultSleepMsDuration = 4'000;
  static constexpr double kDefaultProvisioningFactor = 1.2;

 private:
  bool is_warmed_up_ = false;

  const bool warm_up_once_;
  const size_t sleep_ms_duration_;
  const double provisioning_factor_;
};

class ProvisionedConcurrencyWarmUpStrategy : public WarmUpStrategy {
 public:
  long double WarmUpFunctions(const std::shared_ptr<const Aws::Lambda::LambdaClient>& lambda_client,
                              const std::shared_ptr<const CostCalculator>& cost_calculator,
                              const FunctionConfig& function_config, const size_t concurrency_count) override;
  std::string GetName() const override;

 private:
  std::chrono::time_point<std::chrono::steady_clock> provisioned_concurrency_started_;
  std::chrono::time_point<std::chrono::steady_clock> provisioned_concurrency_last_visited_;
};

}  // namespace skyrise
