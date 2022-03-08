/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#pragma once

#include <optional>
#include <string>
#include <vector>

#include "abstract_lqp_node.hpp"

namespace skyrise {

/**
 * This node type represents a dummy table that is used to project literals.
 * See Projection::DummyTable for more details.
 */
class DummyTableNode : public EnableMakeForPlanNode<DummyTableNode, AbstractLqpNode>, public AbstractLqpNode {
 public:
  DummyTableNode();

  const std::string& Name() const override;
  using AbstractLqpNode::Description;
  std::string Description(const DescriptionMode mode,
                          const AbstractExpression::DescriptionMode expression_mode) const override;

  std::vector<std::shared_ptr<AbstractExpression>> OutputExpressions() const override;
  bool IsColumnNullable(const ColumnId column_id) const override;
  std::shared_ptr<LqpUniqueConstraints> UniqueConstraints() const override;

 protected:
  std::shared_ptr<AbstractLqpNode> OnShallowCopy(LqpNodeMapping& node_mapping) const override;
  bool OnShallowEquals(const AbstractLqpNode& rhs, const LqpNodeMapping& node_mapping) const override;
};

}  // namespace skyrise
