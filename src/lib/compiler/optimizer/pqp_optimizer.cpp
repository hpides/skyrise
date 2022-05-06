#include "pqp_optimizer.hpp"

#include <memory>

//#include "strategy/pqp_combine_partial_aggregates_rule.hpp"
//#include "strategy/pqp_partial_aggregation_rule.hpp"
//#include "strategy/pqp_pipeline_preparation_rule.hpp"

namespace skyrise {

std::shared_ptr<PqpOptimizer> PqpOptimizer::CreateDefaultPqpOptimizer() {
  auto pqp_optimizer = std::make_shared<PqpOptimizer>();

  // TODO(anyone): Enable after rules are merged.
  //  // Divides AggregateOperatorProxy into pre-aggregation and final-aggregation
  //  pqp_optimizer->AddRule(std::make_unique<PqpPartialAggregationRule>());
  //
  //  // Adds data shuffling operations by inserting ExchangeOperatorProxy instances where necessary.
  //  pqp_optimizer->AddRule(std::make_unique<PqpPipelinePreparationRule>());
  //
  //  pqp_optimizer->AddRule(std::make_unique<PqpCombinePartialAggregatesRule>());

  return pqp_optimizer;
}

std::shared_ptr<AbstractOperatorProxy> PqpOptimizer::Optimize(
    std::shared_ptr<AbstractOperatorProxy> pqp_root, const std::shared_ptr<OptimizerMetrics>& optimizer_metrics) const {
  // We cannot allow multiple owners of the PQP as one owner could decide to optimize the plan and others might hold a
  // pointer to a node that is not even part of the plan anymore after optimization. Thus, callers of this method need
  // to relinquish their ownership (i.e., move their shared_ptr into the method) and take ownership of the resulting
  // optimized plan.
  Assert(pqp_root.use_count() == 1, "PqpOptimizer should have exclusive ownership of the given plan.");

  Assert(pqp_root->Type() == OperatorType::kExport, "Expected PQP root node to have OperatorType::kExport.");

  ApplyRules(pqp_root, optimizer_metrics);

  return pqp_root;
}

}  // namespace skyrise
