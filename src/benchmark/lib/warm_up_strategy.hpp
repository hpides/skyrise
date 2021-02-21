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
  explicit WarmUpStrategy(const bool warm_up_once);
  virtual ~WarmUpStrategy() {}

  virtual long double WarmUpFunctions(const std::shared_ptr<Client>& client, const FunctionConfig& function_config,
                                      const size_t function_count);
  virtual long double CalculateWarmUpCost(
      const std::shared_ptr<Client>& client, const FunctionConfig& function_config,
      std::vector<Aws::Lambda::Model::InvokeOutcomeCallable>* invoke_outcome_callables);
  virtual std::shared_ptr<Aws::IOStream> CreateInvokeRequestBody() const = 0;
  virtual std::string GetName() const = 0;

 protected:
  const bool warm_up_once_;
  bool is_warmed_up_;
  std::unique_ptr<CostCalculator> cost_calculator_;
};

class SimpleWarmUpStrategy : public WarmUpStrategy {
 public:
  explicit SimpleWarmUpStrategy(const bool warm_up_once);
  std::shared_ptr<Aws::IOStream> CreateInvokeRequestBody() const override;

  std::string GetName() const override;
};

class SleepWarmUpStrategy : public WarmUpStrategy {
 public:
  explicit SleepWarmUpStrategy(const bool warm_up_once);
  std::shared_ptr<Aws::IOStream> CreateInvokeRequestBody() const override;
  std::string GetName() const override;

 private:
  // TODO(anyone): Eliminate magic number once we understand the parallel running lambda functions better
  const size_t kSleepMs = 7000;
};

class ProvisionedConcurrencyWarmUpStrategy : public WarmUpStrategy {
 public:
  ProvisionedConcurrencyWarmUpStrategy();

  long double WarmUpFunctions(const std::shared_ptr<Client>& client, const FunctionConfig& function_config,
                              const size_t function_count) override;
  std::shared_ptr<Aws::IOStream> CreateInvokeRequestBody() const override;
  std::string GetName() const override;

 private:
  std::chrono::time_point<std::chrono::steady_clock> provisioned_concurrency_started_;
  std::chrono::time_point<std::chrono::steady_clock> provisioned_concurrency_last_visited_;
};

}  // namespace skyrise
