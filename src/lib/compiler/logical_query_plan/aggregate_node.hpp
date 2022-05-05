/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#pragma once

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "abstract_lqp_node.hpp"
#include "types.hpp"

namespace skyrise {

/**
 * This node type is used to describe SELECT lists for statements that have at least one of the following:
 *  - one or more aggregate functions in their SELECT list
 *  - a GROUP BY clause
 *
 *  The order of the output columns is groupby columns followed by aggregate columns
 */
class AggregateNode : public EnableMakeForPlanNode<AggregateNode, AbstractLqpNode>, public AbstractLqpNode {
 public:
  AggregateNode(const std::vector<std::shared_ptr<AbstractExpression>>& group_by_expressions,
                const std::vector<std::shared_ptr<AbstractExpression>>& aggregate_expressions);

  const std::string& Name() const override;
  using AbstractLqpNode::Description;
  std::string Description(const DescriptionMode mode,
                          const AbstractExpression::DescriptionMode expression_mode) const override;

  std::vector<std::shared_ptr<AbstractExpression>> OutputExpressions() const override;
  bool IsColumnNullable(const ColumnId column_id) const override;

  /**
   * (1) Forwards left input node's unique constraints if its expressions are a subset of the group-by expressions.
   * (2) Creates a new unique constraint from the group-by expressions if not already existing.
   */
  std::shared_ptr<LqpUniqueConstraints> UniqueConstraints() const override;

  // Returns non-trivial FDs from the left input node that remain valid.
  std::vector<FunctionalDependency> NonTrivialFunctionalDependencies() const override;

  // node_expression contains both the group_by- and the aggregate_expressions in that order.
  size_t aggregate_expressions_begin_index;

 protected:
  size_t OnShallowHash() const override;
  std::shared_ptr<AbstractLqpNode> OnShallowCopy(LqpNodeMapping& node_mapping) const override;
  bool OnShallowEquals(const AbstractLqpNode& rhs, const LqpNodeMapping& node_mapping) const override;
};

}  // namespace skyrise
