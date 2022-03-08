/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#include "limit_node.hpp"

#include <sstream>
#include <string>

#include "expression/abstract_expression.hpp"
#include "lqp_expression_utils.hpp"
#include "utils/assert.hpp"

namespace skyrise {

LimitNode::LimitNode(const std::shared_ptr<AbstractExpression>& num_rows_expression)
    : AbstractLqpNode(LqpNodeType::kLimit, {num_rows_expression}) {}

const std::string& LimitNode::Name() const {
  static const std::string kName{"Limit"};
  return kName;
}

std::string LimitNode::Description(const DescriptionMode mode,
                                   const AbstractExpression::DescriptionMode expression_mode) const {
  std::stringstream stream;
  const char separator = (mode == DescriptionMode::kSingleLine ? ' ' : '\n');
  stream << "[" << Name() << "]" << separator;
  stream << num_rows_expression()->Description(expression_mode);
  return stream.str();
}

std::shared_ptr<LqpUniqueConstraints> LimitNode::UniqueConstraints() const { return ForwardLeftUniqueConstraints(); }

std::shared_ptr<AbstractExpression> LimitNode::num_rows_expression() const { return node_expressions_[0]; }

std::shared_ptr<AbstractLqpNode> LimitNode::OnShallowCopy(LqpNodeMapping& node_mapping) const {
  return LimitNode::Make(ExpressionCopyAndAdaptToDifferentLqp(*num_rows_expression(), node_mapping));
}

bool LimitNode::OnShallowEquals(const AbstractLqpNode& rhs, const LqpNodeMapping& node_mapping) const {
  const auto& limit_node = static_cast<const LimitNode&>(rhs);
  return ExpressionEqualToExpressionInDifferentLqp(*num_rows_expression(), *limit_node.num_rows_expression(),
                                                   node_mapping);
}

}  // namespace skyrise
