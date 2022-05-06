#include "lqp_optimizer.hpp"

#include <memory>
#include <unordered_set>

#include "compiler/logical_query_plan/logical_plan_root_node.hpp"
//#include "strategy/lqp_average_rewrite_rule.hpp"
//#include "strategy/lqp_column_pruning_rule.hpp"

namespace skyrise {

std::shared_ptr<LqpOptimizer> LqpOptimizer::CreateDefaultLqpOptimizer() {
  auto lqp_optimizer = std::make_shared<LqpOptimizer>();

  //  TODO(anyone): Enable after rules are merged.
  //  lqp_optimizer->AddRule(std::make_unique<LqpAverageRewriteRule>());
  //
  //  lqp_optimizer->AddRule(std::make_unique<LqpColumnPruningRule>());

  return lqp_optimizer;
}

std::shared_ptr<AbstractLqpNode> LqpOptimizer::Optimize(
    std::shared_ptr<AbstractLqpNode> input, const std::shared_ptr<OptimizerMetrics>& optimizer_metrics) const {
  // We cannot allow multiple owners of the LQP as one owner could decide to optimize the plan and others might hold a
  // pointer to a node that is not even part of the plan anymore after optimization. Thus, callers of this method need
  // to relinquish their ownership (i.e., move their shared_ptr into the method) and take ownership of the resulting
  // optimized plan.
  Assert(input.use_count() == 1, "LqpOptimizer should have exclusive ownership of the given plan.");

  // Add explicit root node, so the rules can freely change the tree below it without having to maintain a root node
  // to return to the Optimizer.
  const auto root_node = LogicalPlanRootNode::Make(std::move(input));
  input = nullptr;

  ApplyRules(root_node, optimizer_metrics);

  // Remove LogicalPlanRootNode
  auto optimized_node = root_node->LeftInput();
  root_node->SetLeftInput(nullptr);

  return optimized_node;
}

}  // namespace skyrise
