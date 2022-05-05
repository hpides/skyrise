/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#include "join_node.hpp"

#include <limits>
#include <memory>
#include <numeric>
#include <optional>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include <boost/container_hash/hash.hpp>

#include "expression/binary_predicate_expression.hpp"
#include "expression/lqp_column_expression.hpp"
#include "lqp_expression_utils.hpp"
#include "lqp_utils.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace skyrise {

JoinNode::JoinNode(const JoinMode init_join_mode) : AbstractLqpNode(LqpNodeType::kJoin), join_mode(init_join_mode) {
  Assert(join_mode == JoinMode::kCross, "Only Cross Joins can be constructed without predicate");
}

JoinNode::JoinNode(const JoinMode init_join_mode, const std::shared_ptr<AbstractExpression>& join_predicate)
    : JoinNode(init_join_mode, std::vector<std::shared_ptr<AbstractExpression>>{join_predicate}) {}

JoinNode::JoinNode(const JoinMode init_join_mode,
                   const std::vector<std::shared_ptr<AbstractExpression>>& init_join_predicates)
    : AbstractLqpNode(LqpNodeType::kJoin, init_join_predicates), join_mode(init_join_mode) {
  Assert(join_mode != JoinMode::kCross, "Cross Joins take no predicate");
  Assert(!join_predicates().empty(), "Non-Cross Joins require predicates");
}

const std::string& JoinNode::Name() const {
  static const std::string kName{"Join"};
  return kName;
}

std::string JoinNode::Description(const DescriptionMode mode,
                                  const AbstractExpression::DescriptionMode expression_mode) const {
  std::stringstream stream;
  const char separator = (mode == DescriptionMode::kSingleLine ? ' ' : '\n');
  stream << "[" << Name() << "]" << separator;
  stream << "Mode: " << join_mode;

  for (const auto& predicate : join_predicates()) {
    stream << separator;
    stream << "[" << predicate->Description(expression_mode) << "]";
  }

  return stream.str();
}

bool JoinNode::RequiresRightInput() const { return true; }

std::vector<std::shared_ptr<AbstractExpression>> JoinNode::OutputExpressions() const {
  Assert(LeftInput() && RightInput(), "Both inputs need to be set to determine a JoinNode's output expressions");

  /**
   * Update the JoinNode's output expressions every time they are requested. An overhead, but keeps the LQP code simple.
   * Previously we propagated _input_changed() calls through the LQP every time a node changed and that required a lot
   * of feeble code.
   */

  const auto& left_expressions = LeftInput()->OutputExpressions();
  const auto& right_expressions = RightInput()->OutputExpressions();

  const auto output_both_inputs =
      join_mode != JoinMode::kSemi && join_mode != JoinMode::kAntiNullAsTrue && join_mode != JoinMode::kAntiNullAsFalse;

  std::vector<std::shared_ptr<AbstractExpression>> output_expressions;
  output_expressions.resize(left_expressions.size() + (output_both_inputs ? right_expressions.size() : 0));

  auto right_begin = std::copy(left_expressions.begin(), left_expressions.end(), output_expressions.begin());

  if (output_both_inputs) {
    std::copy(right_expressions.begin(), right_expressions.end(), right_begin);
  }

  return output_expressions;
}

std::shared_ptr<LqpUniqueConstraints> JoinNode::UniqueConstraints() const {
  // Semi- and Anti-Joins act as mere filters for input_left().
  // Therefore, existing unique constraints remain valid.
  if (join_mode == JoinMode::kSemi || join_mode == JoinMode::kAntiNullAsTrue ||
      join_mode == JoinMode::kAntiNullAsFalse) {
    return ForwardLeftUniqueConstraints();
  }

  const auto& left_unique_constraints = LeftInput()->UniqueConstraints();
  const auto& right_unique_constraints = RightInput()->UniqueConstraints();

  return _output_unique_constraints(left_unique_constraints, right_unique_constraints);
}

std::shared_ptr<LqpUniqueConstraints> JoinNode::_output_unique_constraints(
    const std::shared_ptr<LqpUniqueConstraints>& left_unique_constraints,
    const std::shared_ptr<LqpUniqueConstraints>& right_unique_constraints) const {
  if (left_unique_constraints->empty() && right_unique_constraints->empty()) {
    // Early exit
    return std::make_shared<LqpUniqueConstraints>();
  }

  const auto predicates = join_predicates();
  if (predicates.empty() || predicates.size() > 1) {
    // No guarantees implemented yet for Cross Joins and multi-predicate joins
    return std::make_shared<LqpUniqueConstraints>();
  }

  DebugAssert(join_mode == JoinMode::kInner || join_mode == JoinMode::kLeftOuter ||
                  join_mode == JoinMode::kRightOuter || join_mode == JoinMode::kFullOuter,
              "Unhandled JoinMode");

  const auto join_predicate = std::dynamic_pointer_cast<BinaryPredicateExpression>(join_predicates().front());
  if (!join_predicate || join_predicate->predicate_condition_ != PredicateCondition::kEquals) {
    // Also, no guarantees implemented yet for other join predicates than _equals() (Equi Join)
    return std::make_shared<LqpUniqueConstraints>();
  }

  // Check uniqueness of join columns
  bool left_operand_is_unique =
      !left_unique_constraints->empty() &&
      ContainsMatchingUniqueConstraint(left_unique_constraints, {join_predicate->LeftOperand()});
  bool right_operand_is_unique =
      !right_unique_constraints->empty() &&
      ContainsMatchingUniqueConstraint(right_unique_constraints, {join_predicate->RightOperand()});

  if (left_operand_is_unique && right_operand_is_unique) {
    // Due to the one-to-one relationship, the constraints of both sides remain valid.
    auto unique_constraints =
        std::make_shared<LqpUniqueConstraints>(left_unique_constraints->begin(), left_unique_constraints->end());
    std::copy(right_unique_constraints->begin(), right_unique_constraints->end(),
              std::back_inserter(*unique_constraints));
    return unique_constraints;

  } else if (left_operand_is_unique) {
    // Uniqueness on the left prevents duplication of records on the right
    return right_unique_constraints;
  } else if (right_operand_is_unique) {
    // Uniqueness on the right prevents duplication of records on the left
    return left_unique_constraints;
  }

  return std::make_shared<LqpUniqueConstraints>();
}

std::vector<FunctionalDependency> JoinNode::NonTrivialFunctionalDependencies() const {
  /**
   * In the case of Semi- & Anti-Joins, this node acts as a filter for the left input node. The number of output
   * expressions does not change and therefore we should forward non-trivial FDs as follows:
   */
  if (join_mode == JoinMode::kSemi || join_mode == JoinMode::kAntiNullAsTrue ||
      join_mode == JoinMode::kAntiNullAsFalse) {
    return LeftInput()->NonTrivialFunctionalDependencies();
  }

  /**
   * When joining tables, we usually lose some or even all unique constraints from both input tables. This leads to
   * fewer trivial FDs that we can generate from unique constraints in upper nodes.
   * To preserve all FDs possible, we manually forward all FDs from input nodes, which unique constraints become
   * discarded.
   */
  auto fds_left = std::vector<FunctionalDependency>();
  auto fds_right = std::vector<FunctionalDependency>();

  const auto left_unique_constraints = LeftInput()->UniqueConstraints();
  const auto right_unique_constraints = RightInput()->UniqueConstraints();
  const auto& output_unique_constraints = _output_unique_constraints(left_unique_constraints, right_unique_constraints);

  if (output_unique_constraints->empty() && !left_unique_constraints->empty() && !right_unique_constraints->empty()) {
    // Left and Right unique constraints become discarded, so we have to manually forward all FDs from the input nodes.
    fds_left = LeftInput()->FunctionalDependencies();
    fds_right = RightInput()->FunctionalDependencies();
  } else if ((output_unique_constraints->empty() || output_unique_constraints == right_unique_constraints) &&
             !left_unique_constraints->empty()) {
    // Left unique constraints become discarded, so we have to manually forward all left input node's FDs
    fds_left = LeftInput()->FunctionalDependencies();
    fds_right = RightInput()->NonTrivialFunctionalDependencies();
  } else if ((output_unique_constraints->empty() || output_unique_constraints == left_unique_constraints) &&
             !right_unique_constraints->empty()) {
    // Right unique constraints become discarded, so we have to manually forward all right input node's FDs
    fds_left = LeftInput()->NonTrivialFunctionalDependencies();
    fds_right = RightInput()->FunctionalDependencies();
  } else {
    // No unique constraints become discarded. We only have to forward non-trivial FDs.
    DebugAssert(
        output_unique_constraints->size() == (left_unique_constraints->size() + right_unique_constraints->size()),
        "Unexpected number of unique constraints.");
    fds_left = LeftInput()->NonTrivialFunctionalDependencies();
    fds_right = RightInput()->NonTrivialFunctionalDependencies();
  }

  // Prevent FDs with duplicate determinant expressions in the output vector
  auto fds_out = UnionFds(fds_left, fds_right);

  // Outer joins lead to nullable columns, which may invalidate some FDs
  if (!fds_out.empty() &&
      (join_mode == JoinMode::kFullOuter || join_mode == JoinMode::kLeftOuter || join_mode == JoinMode::kRightOuter)) {
    RemoveInvalidFds(SharedFromBase(), fds_out);
  }

  /**
   * Future Work: In some cases, it is possible to create FDs from the join columns.
   *              For example: a) {join_column_a} => {join_column_b}
   *                           b) {join_column_b} => {join_column_a}
   */

  return fds_out;
}

bool JoinNode::IsColumnNullable(const ColumnId column_id) const {
  Assert(LeftInput() && RightInput(), "Need both inputs to determine nullability");

  const auto left_input_column_count = LeftInput()->OutputExpressions().size();
  const auto column_is_from_left_input = column_id < left_input_column_count;

  if (join_mode == JoinMode::kLeftOuter && !column_is_from_left_input) {
    return true;
  }

  if (join_mode == JoinMode::kRightOuter && column_is_from_left_input) {
    return true;
  }

  if (join_mode == JoinMode::kFullOuter) {
    return true;
  }

  if (column_is_from_left_input) {
    return LeftInput()->IsColumnNullable(column_id);
  } else {
    ColumnId right_column_id = column_id - static_cast<ColumnId>(left_input_column_count);
    return RightInput()->IsColumnNullable(right_column_id);
  }
}

const std::vector<std::shared_ptr<AbstractExpression>>& JoinNode::join_predicates() const { return node_expressions_; }

size_t JoinNode::OnShallowHash() const { return boost::hash_value(join_mode); }

std::shared_ptr<AbstractLqpNode> JoinNode::OnShallowCopy(LqpNodeMapping& node_mapping) const {
  if (!join_predicates().empty()) {
    return JoinNode::Make(join_mode, ExpressionsCopyAndAdaptToDifferentLqp(join_predicates(), node_mapping));
  } else {
    return JoinNode::Make(join_mode);
  }
}

bool JoinNode::OnShallowEquals(const AbstractLqpNode& rhs, const LqpNodeMapping& node_mapping) const {
  const auto& join_node = static_cast<const JoinNode&>(rhs);
  if (join_mode != join_node.join_mode) {
    return false;
  }
  return ExpressionsEqualToExpressionsInDifferentLqp(join_predicates(), join_node.join_predicates(), node_mapping);
}

}  // namespace skyrise
