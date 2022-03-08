/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_lqp_node.hpp"
#include "types.hpp"

namespace skyrise {

/**
 * This node type is used to represent the UNION set operation in two modes, Unique and All.
 */
class UnionNode : public EnableMakeForPlanNode<UnionNode, AbstractLqpNode>, public AbstractLqpNode {
 public:
  explicit UnionNode(const SetOperationMode init_set_operation_mode);

  const std::string& Name() const override;
  using AbstractLqpNode::Description;
  std::string Description(const DescriptionMode mode,
                          const AbstractExpression::DescriptionMode expression_mode) const override;

  bool RequiresRightInput() const override;
  std::vector<std::shared_ptr<AbstractExpression>> OutputExpressions() const override;
  bool IsColumnNullable(const ColumnId column_id) const override;

  /**
   * (1) Discards all input unique constraints for SetOperationMode::kAll
   * (2) Fails for SetOperationMode::kUnique, which is not yet implemented.
   */
  std::shared_ptr<LqpUniqueConstraints> UniqueConstraints() const override;

  // Implementation is limited to SetOperationMode::kAll only.
  std::vector<FunctionalDependency> NonTrivialFunctionalDependencies() const override;

  const SetOperationMode set_operation_mode;

 protected:
  size_t OnShallowHash() const override;
  std::shared_ptr<AbstractLqpNode> OnShallowCopy(LqpNodeMapping& node_mapping) const override;
  bool OnShallowEquals(const AbstractLqpNode& rhs, const LqpNodeMapping& node_mapping) const override;
};
}  // namespace skyrise
