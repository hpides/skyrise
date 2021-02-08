#pragma once

#include <string>

#include "client/client.hpp"

namespace skyrise {

// TODO(anyone): Move the best strategy to src/lib/scheduler

class WarmUpStrategy {
 public:
  explicit WarmUpStrategy(const bool warm_up_once);
  virtual ~WarmUpStrategy() {}

  virtual void WarmUpFunctions(const std::shared_ptr<Client>& client, const std::string& function_name,
                               const size_t function_count) = 0;
  virtual std::string GetName() const = 0;

  const bool warm_up_once_;
  bool was_warmed_up_;
};

class SimpleWarmUpStrategy : public WarmUpStrategy {
 public:
  explicit SimpleWarmUpStrategy(const bool warm_up_once);
  void WarmUpFunctions(const std::shared_ptr<Client>& client, const std::string& function_name,
                       const size_t function_count) override;
  std::string GetName() const override;
};

class SleepWarmUpStrategy : public WarmUpStrategy {
 public:
  explicit SleepWarmUpStrategy(const bool warm_up_once);

  void WarmUpFunctions(const std::shared_ptr<Client>& client, const std::string& function_name,
                       const size_t function_count) override;

  std::string GetName() const override;
};

}  // namespace skyrise
