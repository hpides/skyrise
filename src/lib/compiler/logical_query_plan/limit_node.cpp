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

LimitNode::LimitNode(const std::shared_ptr<AbstractExpression>& number_of_rows_expression)
    : AbstractLqpNode(LqpNodeType::kLimit, {number_of_rows_expression}) {}

const std::string& LimitNode::Name() const {
  static const std::string kName = "Limit";
  return kName;
}

std::string LimitNode::Description(const DescriptionMode mode,
                                   const AbstractExpression::DescriptionMode expression_mode) const {
  std::stringstream stream;
  const char separator = (mode == DescriptionMode::kSingleLine ? ' ' : '\n');
  stream << "[" << Name() << "]" << separator;
  stream << NumberOfRowsExpression()->Description(expression_mode);
  return stream.str();
}

std::shared_ptr<LqpUniqueConstraints> LimitNode::UniqueConstraints() const { return ForwardLeftUniqueConstraints(); }

std::shared_ptr<AbstractExpression> LimitNode::NumberOfRowsExpression() const { return node_expressions_[0]; }

std::shared_ptr<AbstractLqpNode> LimitNode::OnShallowCopy(LqpNodeMapping& node_mapping) const {
  return LimitNode::Make(ExpressionCopyAndAdaptToDifferentLqp(*NumberOfRowsExpression(), node_mapping));
}

bool LimitNode::OnShallowEquals(const AbstractLqpNode& rhs, const LqpNodeMapping& node_mapping) const {
  const auto& limit_node = static_cast<const LimitNode&>(rhs);
  return ExpressionEqualToExpressionInDifferentLqp(*NumberOfRowsExpression(), *limit_node.NumberOfRowsExpression(),
                                                   node_mapping);
}

}  // namespace skyrise
