/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#include "predicate_node.hpp"

#include <memory>
#include <optional>
#include <sstream>
#include <string>

#include <boost/container_hash/hash.hpp>

#include "expression/between_expression.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "expression/lqp_column_expression.hpp"
#include "expression/value_expression.hpp"
#include "lqp_expression_utils.hpp"
//#include "operators/operator_scan_predicate.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace skyrise {

PredicateNode::PredicateNode(const std::shared_ptr<AbstractExpression>& predicate)
    : AbstractLqpNode(LqpNodeType::kPredicate, {predicate}) {}

const std::string& PredicateNode::Name() const {
  static const std::string kName{"Predicate"};
  return kName;
}

std::string PredicateNode::Description(const DescriptionMode mode,
                                       const AbstractExpression::DescriptionMode expression_mode) const {
  std::stringstream stream;
  const char separator = (mode == DescriptionMode::kSingleLine ? ' ' : '\n');
  stream << "[" << Name() << "]" << separator;
  stream << predicate()->Description(expression_mode);
  return stream.str();
}

std::shared_ptr<LqpUniqueConstraints> PredicateNode::UniqueConstraints() const {
  return ForwardLeftUniqueConstraints();
}

std::shared_ptr<AbstractExpression> PredicateNode::predicate() const { return node_expressions_[0]; }

size_t PredicateNode::OnShallowHash() const { return boost::hash_value(scan_type); }

std::shared_ptr<AbstractLqpNode> PredicateNode::OnShallowCopy(LqpNodeMapping& node_mapping) const {
  return std::make_shared<PredicateNode>(ExpressionCopyAndAdaptToDifferentLqp(*predicate(), node_mapping));
}

bool PredicateNode::OnShallowEquals(const AbstractLqpNode& rhs, const LqpNodeMapping& node_mapping) const {
  const auto& predicate_node = static_cast<const PredicateNode&>(rhs);
  const auto equal = ExpressionEqualToExpressionInDifferentLqp(*predicate(), *predicate_node.predicate(), node_mapping);

  return equal;
}

}  // namespace skyrise
