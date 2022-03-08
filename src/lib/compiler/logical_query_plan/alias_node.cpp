/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#include "alias_node.hpp"

#include <sstream>

#include <boost/container_hash/hash.hpp>

#include "lqp_expression_utils.hpp"

namespace skyrise {

AliasNode::AliasNode(const std::vector<std::shared_ptr<AbstractExpression>>& expressions,
                     const std::vector<std::string>& init_aliases)
    : AbstractLqpNode(LqpNodeType::kAlias, expressions), aliases(init_aliases) {
  Assert(expressions.size() == aliases.size(), "Number of expressions and number of aliases has to be equal.");
}

const std::string& AliasNode::Name() const {
  static const std::string kName{"Alias"};
  return kName;
}

std::string AliasNode::Description(const DescriptionMode mode,
                                   const AbstractExpression::DescriptionMode expression_mode) const {
  std::stringstream stream;
  const char separator = (mode == DescriptionMode::kSingleLine ? ' ' : '\n');
  stream << "[" << Name() << "]" << separator;

  for (auto column_id = ColumnId{0}; column_id < node_expressions_.size(); ++column_id) {
    if (node_expressions_[column_id]->Description(expression_mode) == aliases[column_id]) {
      stream << aliases[column_id];
    } else {
      stream << node_expressions_[column_id]->Description(expression_mode) << " AS " << aliases[column_id];
    }

    if (column_id + 1u < node_expressions_.size()) stream << ", ";
  }
  return stream.str();
}

std::vector<std::shared_ptr<AbstractExpression>> AliasNode::OutputExpressions() const { return node_expressions_; }

std::shared_ptr<LqpUniqueConstraints> AliasNode::UniqueConstraints() const { return ForwardLeftUniqueConstraints(); }

size_t AliasNode::OnShallowHash() const {
  size_t hash{0};
  for (const auto& alias : aliases) {
    boost::hash_combine(hash, alias);
  }
  return hash;
}

std::shared_ptr<AbstractLqpNode> AliasNode::OnShallowCopy(LqpNodeMapping& node_mapping) const {
  return std::make_shared<AliasNode>(ExpressionsCopyAndAdaptToDifferentLqp(node_expressions_, node_mapping), aliases);
}

bool AliasNode::OnShallowEquals(const AbstractLqpNode& rhs, const LqpNodeMapping& node_mapping) const {
  const auto& alias_node = static_cast<const AliasNode&>(rhs);
  return ExpressionsEqualToExpressionsInDifferentLqp(node_expressions_, alias_node.node_expressions_, node_mapping) &&
         aliases == alias_node.aliases;
}

}  // namespace skyrise
