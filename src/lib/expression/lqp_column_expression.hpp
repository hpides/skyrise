/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#pragma once

#include "abstract_expression.hpp"
#include "aggregate_expression.hpp"
#include "compiler/logical_query_plan/abstract_lqp_node.hpp"
#include "types.hpp"

namespace skyrise {

class LqpColumnExpression : public AbstractExpression {
 public:
  explicit LqpColumnExpression(const std::shared_ptr<const AbstractLqpNode>& original_node,
                               const ColumnId original_column_id);

  std::shared_ptr<AbstractExpression> DeepCopy() const override;
  std::string Description(const DescriptionMode mode) const override;
  DataType GetDataType() const override;
  bool RequiresComputation() const override;

  // Needs to be weak since nodes can store LqpColumnExpressions referring to themselves (e.g., for
  // StoredTableNode::output_expressions). If the original_node is not referenced by any shared_ptr anymore, it is
  // deleted. As a result, the weak_ptr expires. It should not be accessed anymore. Thus, if original_node.lock() is
  // a nullptr, the LQP is defective.
  const std::weak_ptr<const AbstractLqpNode> original_node_;
  const ColumnId original_column_id_;

 protected:
  bool ShallowEquals(const AbstractExpression& expression) const override;
  size_t ShallowHash() const override;
};

/**
 * Semantically, the following two functions belong to expression_functional.hpp. However, since Coordinator and Worker
 * sources should stay strictly separate, both functions were moved here.
 */
std::shared_ptr<LqpColumnExpression> LqpColumn_(const std::shared_ptr<const AbstractLqpNode>& original_node,
                                                const ColumnId original_column_id);

std::shared_ptr<AggregateExpression> CountStarLqp_(const std::shared_ptr<AbstractLqpNode>& lqp_node);

}  // namespace skyrise
