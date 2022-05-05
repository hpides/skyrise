/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_lqp_node.hpp"

namespace skyrise {

/**
 * Assign column names to expressions
 */
class AliasNode : public EnableMakeForPlanNode<AliasNode, AbstractLqpNode>, public AbstractLqpNode {
 public:
  AliasNode(const std::vector<std::shared_ptr<AbstractExpression>>& expressions,
            const std::vector<std::string>& aliases);

  const std::string& Name() const override;
  using AbstractLqpNode::Description;
  std::string Description(const DescriptionMode mode,
                          const AbstractExpression::DescriptionMode expression_mode) const override;

  std::vector<std::shared_ptr<AbstractExpression>> OutputExpressions() const override;

  // Forwards unique constraints from the left input node
  std::shared_ptr<LqpUniqueConstraints> UniqueConstraints() const override;

  const std::vector<std::string> aliases_;

 protected:
  size_t OnShallowHash() const override;
  std::shared_ptr<AbstractLqpNode> OnShallowCopy(LqpNodeMapping& node_mapping) const override;
  bool OnShallowEquals(const AbstractLqpNode& rhs, const LqpNodeMapping& node_mapping) const override;
};

}  // namespace skyrise
