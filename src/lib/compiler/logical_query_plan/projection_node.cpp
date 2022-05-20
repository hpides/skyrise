/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#include "projection_node.hpp"

#include <sstream>

#include "lqp_expression_utils.hpp"
#include "lqp_utils.hpp"
#include "utils/assert.hpp"

namespace skyrise {

ProjectionNode::ProjectionNode(const std::vector<std::shared_ptr<AbstractExpression>>& expressions)
    : AbstractLqpNode(LqpNodeType::kProjection, expressions) {}

const std::string& ProjectionNode::Name() const {
  static const std::string kName = "Projection";
  return kName;
}

std::string ProjectionNode::Description(const DescriptionMode mode,
                                        const AbstractExpression::DescriptionMode expression_mode) const {
  std::stringstream stream;
  const char separator = (mode == DescriptionMode::kSingleLine ? ' ' : '\n');
  stream << "[" << Name() << "]" << separator;
  stream << ExpressionDescriptions(node_expressions_, expression_mode);
  return stream.str();
}

std::vector<std::shared_ptr<AbstractExpression>> ProjectionNode::OutputExpressions() const { return node_expressions_; }

bool ProjectionNode::IsColumnNullable(const ColumnId column_id) const {
  Assert(column_id < node_expressions_.size(), "ColumnId out of range");
  Assert(LeftInput(), "Need left input to determine nullability");
  return ExpressionIsNullableOnLqp(node_expressions_[column_id], *LeftInput());
}

std::shared_ptr<LqpUniqueConstraints> ProjectionNode::UniqueConstraints() const {
  auto unique_constraints = std::make_shared<LqpUniqueConstraints>();
  unique_constraints->reserve(node_expressions_.size());

  // Forward unique constraints, if applicable
  const auto& input_unique_constraints = LeftInput()->UniqueConstraints();

  for (const auto& input_unique_constraint : *input_unique_constraints) {
    if (!HasOutputExpressions(input_unique_constraint.expressions)) {
      continue;
      /**
       * Future Work:
       * Our implementation does not exploit all opportunities yet.
       * As the next step, we could check for derived output expressions that preserve uniqueness, for example,
       * the expression 'column + 1'.
       * Instead of discarding a unique constraint for 'column', we could create and output a new one for 'column + 1'.
       */
    }
    unique_constraints->emplace_back(input_unique_constraint);
  }

  return unique_constraints;
}

std::vector<FunctionalDependency> ProjectionNode::NonTrivialFunctionalDependencies() const {
  auto non_trivial_fds = LeftInput()->NonTrivialFunctionalDependencies();

  // Currently, we remove non-trivial FDs whose expressions are no longer part of the node's output expressions.
  RemoveInvalidFunctionalDependencies(SharedFromBase(), non_trivial_fds);

  /**
   * Future Work: By analyzing the output expressions in more depth, we can save some of the input FDs. For example:
   *               - StoredTableNode with the columns a, b, c and the following FD:
   *                 {a} -> {b}
   *               - ProjectionNode with the following output expressions:
   *                 a, (b + 1)
   *
   *              The current implementation discards the above FD. Instead, we could save it via the following
   *              reformulation: {a} -> {b + 1}
   */

  return non_trivial_fds;
}

std::shared_ptr<AbstractLqpNode> ProjectionNode::OnShallowCopy(LqpNodeMapping& node_mapping) const {
  return Make(ExpressionsCopyAndAdaptToDifferentLqp(node_expressions_, node_mapping));
}

bool ProjectionNode::OnShallowEquals(const AbstractLqpNode& rhs, const LqpNodeMapping& node_mapping) const {
  const auto& rhs_expressions = static_cast<const ProjectionNode&>(rhs).node_expressions_;
  return ExpressionsEqualToExpressionsInDifferentLqp(node_expressions_, rhs_expressions, node_mapping);
}

}  // namespace skyrise
