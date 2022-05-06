#pragma once

#include <algorithm>
#include <memory>
#include <vector>

#include "abstract_optimizer.hpp"
#include "abstract_rule.hpp"
#include "compiler/logical_query_plan/abstract_lqp_node.hpp"

namespace skyrise {

class LqpOptimizer final : public AbstractOptimizer<AbstractLqpNode> {
 public:
  LqpOptimizer() = default;

  static std::shared_ptr<LqpOptimizer> CreateDefaultLqpOptimizer();

  /**
   * Optimizes the given @param input by applying all optimizer rules.
   * @param optimizer_metrics may be set in order to retrieve runtime information for each applied rule.
   * @returns the optimized LQP.
   */
  std::shared_ptr<AbstractLqpNode> Optimize(std::shared_ptr<AbstractLqpNode> input,
                                            const std::shared_ptr<OptimizerMetrics>& optimizer_metrics = nullptr) const;
};

}  // namespace skyrise
