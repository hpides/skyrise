/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#include "sort_node.hpp"

#include <sstream>
#include <string>
#include <vector>

#include <boost/container_hash/hash.hpp>

#include "lqp_expression_utils.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace skyrise {

SortNode::SortNode(const std::vector<std::shared_ptr<AbstractExpression>>& expressions,
                   const std::vector<SortMode>& init_sort_modes)
    : AbstractLqpNode(LqpNodeType::kSort, expressions), sort_modes(init_sort_modes) {
  Assert(expressions.size() == sort_modes.size(), "Expected as many Expressions as SortModes");
}

const std::string& SortNode::Name() const {
  static const std::string kName{"Sort"};
  return kName;
}

std::string SortNode::Description(const DescriptionMode mode,
                                  const AbstractExpression::DescriptionMode expression_mode) const {
  std::stringstream stream;
  const char separator = (mode == DescriptionMode::kSingleLine ? ' ' : '\n');
  stream << "[" << Name() << "]" << separator;

  for (size_t i = 0; i < node_expressions_.size(); ++i) {
    stream << node_expressions_[i]->Description(expression_mode) << " ";
    stream << "(" << sort_modes[i] << ")";

    if (i + 1 < node_expressions_.size()) {
      stream << "," << separator;
    }
  }
  return stream.str();
}

std::shared_ptr<LqpUniqueConstraints> SortNode::UniqueConstraints() const { return ForwardLeftUniqueConstraints(); }

size_t SortNode::OnShallowHash() const {
  size_t hash{0};
  for (const auto& sort_mode : sort_modes) {
    boost::hash_combine(hash, sort_mode);
  }
  return hash;
}

std::shared_ptr<AbstractLqpNode> SortNode::OnShallowCopy(LqpNodeMapping& node_mapping) const {
  return SortNode::Make(ExpressionsCopyAndAdaptToDifferentLqp(node_expressions_, node_mapping), sort_modes);
}

bool SortNode::OnShallowEquals(const AbstractLqpNode& rhs, const LqpNodeMapping& node_mapping) const {
  const auto& sort_node = static_cast<const SortNode&>(rhs);

  return ExpressionsEqualToExpressionsInDifferentLqp(node_expressions_, sort_node.node_expressions_, node_mapping) &&
         sort_modes == sort_node.sort_modes;
}

}  // namespace skyrise
