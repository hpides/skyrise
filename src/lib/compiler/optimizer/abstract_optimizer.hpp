#pragma once

#include <chrono>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "abstract_rule.hpp"
#include "compiler/abstract_plan_node.hpp"
#include "utils/assert.hpp"
#include "utils/timer.hpp"

namespace skyrise {

struct OptimizerMetrics {
  OptimizerMetrics() = default;

  void AddRuleDuration(std::string rule_name, std::chrono::nanoseconds duration) {
    Assert(rule_durations.find(rule_name) == rule_durations.end(),
           "Metrics for the given rule name '" + rule_name + "' were already stored.");
    rule_durations.emplace(std::move(rule_name), duration);
  }

  size_t RuleDurationsCount() { return rule_durations.size(); }
  const std::unordered_map<std::string, std::chrono::nanoseconds>& RuleDurations() { return rule_durations; }

 private:
  std::unordered_map<std::string, std::chrono::nanoseconds> rule_durations;
};

template <class NodeType>
class AbstractOptimizer {
 public:
  virtual ~AbstractOptimizer() = default;

  void AddRule(std::unique_ptr<AbstractRule> rule) { rules_.emplace_back(std::move(rule)); }

 protected:
  void ApplyRules(const std::shared_ptr<NodeType>& plan_root,
                  const std::shared_ptr<OptimizerMetrics>& optimizer_metrics = nullptr) const {
    for (const auto& rule : rules_) {
      Timer timer;
      rule->ApplyTo(plan_root);
      auto rule_duration = timer.Lap();

      if (optimizer_metrics) {
        optimizer_metrics->AddRuleDuration(rule->Name(), rule_duration);
      }
    }
  }

 private:
  std::vector<std::unique_ptr<AbstractRule>> rules_;
};

}  // namespace skyrise
