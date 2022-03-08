/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#pragma once

#include <string>

#include "abstract_lqp_node.hpp"

namespace skyrise {

/**
 * This node type represents limiting a result to a certain number of rows (LIMIT operator).
 */
class LimitNode : public EnableMakeForPlanNode<LimitNode, AbstractLqpNode>, public AbstractLqpNode {
 public:
  explicit LimitNode(const std::shared_ptr<AbstractExpression>& num_rows_expression);

  const std::string& Name() const override;
  using AbstractLqpNode::Description;
  std::string Description(const DescriptionMode mode,
                          const AbstractExpression::DescriptionMode expression_mode) const override;

  // Forwards unique constraints from the left input node
  std::shared_ptr<LqpUniqueConstraints> UniqueConstraints() const override;

  std::shared_ptr<AbstractExpression> num_rows_expression() const;

 protected:
  std::shared_ptr<AbstractLqpNode> OnShallowCopy(LqpNodeMapping& node_mapping) const override;
  bool OnShallowEquals(const AbstractLqpNode& rhs, const LqpNodeMapping& node_mapping) const override;
};

}  // namespace skyrise
