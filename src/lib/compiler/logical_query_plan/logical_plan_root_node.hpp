/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#pragma once

#include <string>

#include "abstract_lqp_node.hpp"

namespace skyrise {

/**
 * This node is used in the Optimizer to have an explicit root node it can hold onto the LQP with.
 *
 * Optimizer rules are not allowed to remove this node or add nodes above it.
 *
 * By that Optimizer Rules don't have to worry whether they change the tree-identifying root node,
 * e.g. by removing the Projection at the top of the tree.
 */
class LogicalPlanRootNode : public EnableMakeForPlanNode<LogicalPlanRootNode, AbstractLqpNode>, public AbstractLqpNode {
 public:
  LogicalPlanRootNode();

  const std::string& Name() const override;
  using AbstractLqpNode::Description;
  std::string Description(const DescriptionMode mode,
                          const AbstractExpression::DescriptionMode expression_mode) const override;

  std::shared_ptr<LqpUniqueConstraints> UniqueConstraints() const override;
  std::vector<FunctionalDependency> NonTrivialFunctionalDependencies() const override;

 protected:
  std::shared_ptr<AbstractLqpNode> OnShallowCopy(LqpNodeMapping& node_mapping) const override;
  bool OnShallowEquals(const AbstractLqpNode& rhs, const LqpNodeMapping& node_mapping) const override;
};

}  // namespace skyrise
