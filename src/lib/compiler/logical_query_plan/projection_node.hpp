/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#pragma once

#include <vector>

#include "abstract_lqp_node.hpp"
#include "expression/abstract_expression.hpp"

namespace skyrise {

class ProjectionNode : public EnableMakeForPlanNode<ProjectionNode, AbstractLqpNode>, public AbstractLqpNode {
 public:
  explicit ProjectionNode(const std::vector<std::shared_ptr<AbstractExpression>>& expressions);

  const std::string& Name() const override;
  using AbstractLqpNode::Description;
  std::string Description(const DescriptionMode mode,
                          const AbstractExpression::DescriptionMode expression_mode) const override;

  std::vector<std::shared_ptr<AbstractExpression>> OutputExpressions() const override;
  bool IsColumnNullable(const ColumnId column_id) const override;

  /**
   * Forwards unique constraints from the left input node that fulfill the following criteria:
   *  - unique constraint's expressions remain part of the ProjectionNode's output expressions
   */
  std::shared_ptr<LqpUniqueConstraints> UniqueConstraints() const override;

  // Returns non-trivial FDs from the left input node that remain valid.
  std::vector<FunctionalDependency> NonTrivialFunctionalDependencies() const override;

 protected:
  std::shared_ptr<AbstractLqpNode> OnShallowCopy(LqpNodeMapping& node_mapping) const override;
  bool OnShallowEquals(const AbstractLqpNode& rhs, const LqpNodeMapping& node_mapping) const override;
};

}  // namespace skyrise
