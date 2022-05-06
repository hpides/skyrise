#pragma once

#include <algorithm>
#include <memory>
#include <vector>

#include "abstract_optimizer.hpp"
#include "abstract_rule.hpp"
#include "compiler/physical_query_plan/abstract_operator_proxy.hpp"

namespace skyrise {

class PqpOptimizer final : public AbstractOptimizer<AbstractOperatorProxy> {
 public:
  PqpOptimizer() = default;  // TODO(julianmenzler): Need for QueryContext?

  static std::shared_ptr<PqpOptimizer> CreateDefaultPqpOptimizer();

  /**
   * Optimizes the given @param pqp_root by applying all optimizer rules.
   * @param optimizer_metrics may be set in order to retrieve runtime information for each applied rule.
   * @returns the optimized PQP.
   */
  std::shared_ptr<AbstractOperatorProxy> Optimize(
      std::shared_ptr<AbstractOperatorProxy> pqp_root,
      const std::shared_ptr<OptimizerMetrics>& optimizer_metrics = nullptr) const;
};

}  // namespace skyrise
