/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#include "lqp_utils.hpp"

#include "expression/abstract_expression.hpp"
#include "expression/lqp_column_expression.hpp"
#include "lqp_expression_utils.hpp"
#include "utils/assert.hpp"

namespace {

using namespace skyrise;  // NOLINT(google-build-using-namespace)

void lqp_create_node_mapping_impl(LqpNodeMapping& mapping, const std::shared_ptr<AbstractLqpNode>& lhs,
                                  const std::shared_ptr<AbstractLqpNode>& rhs) {
  if (!lhs && !rhs) return;

  Assert(lhs && rhs, "LQPs aren't equally structured, can't create mapping.");
  Assert(lhs->Type() == rhs->Type(), "LQPs aren't equally structured, can't create mapping.");

  // To avoid traversing subgraphs of ORs twice, check whether we've been here already
  const auto mapping_iter = mapping.find(lhs);
  if (mapping_iter != mapping.end()) return;

  mapping[lhs] = rhs;

  lqp_create_node_mapping_impl(mapping, lhs->LeftInput(), rhs->LeftInput());
  lqp_create_node_mapping_impl(mapping, lhs->RightInput(), rhs->RightInput());
}

std::optional<LqpMismatch> lqp_find_structure_mismatch(const std::shared_ptr<const AbstractLqpNode>& lhs,
                                                       const std::shared_ptr<const AbstractLqpNode>& rhs) {
  if (!lhs && !rhs) return std::nullopt;
  if (!(lhs && rhs) || lhs->Type() != rhs->Type()) return LqpMismatch(lhs, rhs);

  auto mismatch_left = lqp_find_structure_mismatch(lhs->LeftInput(), rhs->LeftInput());
  if (mismatch_left) return mismatch_left;

  return lqp_find_structure_mismatch(lhs->RightInput(), rhs->RightInput());
}

std::optional<LqpMismatch> lqp_find_subplan_mismatch_impl(const LqpNodeMapping& node_mapping,
                                                          const std::shared_ptr<const AbstractLqpNode>& lhs,
                                                          const std::shared_ptr<const AbstractLqpNode>& rhs) {
  if (!lhs && !rhs) return std::nullopt;
  if (!lhs->ShallowEquals(*rhs, node_mapping)) return LqpMismatch(lhs, rhs);

  auto mismatch_left = lqp_find_subplan_mismatch_impl(node_mapping, lhs->LeftInput(), rhs->LeftInput());
  if (mismatch_left) return mismatch_left;

  return lqp_find_subplan_mismatch_impl(node_mapping, lhs->RightInput(), rhs->RightInput());
}

}  // namespace

namespace skyrise {

LqpNodeMapping lqp_create_node_mapping(const std::shared_ptr<AbstractLqpNode>& lhs,
                                       const std::shared_ptr<AbstractLqpNode>& rhs) {
  LqpNodeMapping mapping;
  lqp_create_node_mapping_impl(mapping, lhs, rhs);
  return mapping;
}

std::optional<LqpMismatch> lqp_find_subplan_mismatch(const std::shared_ptr<const AbstractLqpNode>& lhs,
                                                     const std::shared_ptr<const AbstractLqpNode>& rhs) {
  // Check for type/structural mismatched
  auto mismatch = lqp_find_structure_mismatch(lhs, rhs);
  if (mismatch) return mismatch;

  // For lqp_create_node_mapping() we need mutable pointers - but won't use them to manipulate, promised.
  // It's just that NodeMapping has takes a mutable ptr in as the value type
  const auto mutable_lhs = std::const_pointer_cast<AbstractLqpNode>(lhs);
  const auto mutable_rhs = std::const_pointer_cast<AbstractLqpNode>(rhs);
  const auto node_mapping = lqp_create_node_mapping(mutable_lhs, mutable_rhs);

  return lqp_find_subplan_mismatch_impl(node_mapping, lhs, rhs);
}

std::vector<std::shared_ptr<AbstractLqpNode>> LqpFindNodesByType(const std::shared_ptr<AbstractLqpNode>& lqp,
                                                                 const LqpNodeType type) {
  std::vector<std::shared_ptr<AbstractLqpNode>> nodes;
  VisitLqp(lqp, [&](const auto& node) {
    if (node->Type() == type) {
      nodes.emplace_back(node);
    }
    return LqpVisitation::kVisitInputs;
  });

  return nodes;
}

std::vector<std::shared_ptr<AbstractLqpNode>> lqp_find_leaves(const std::shared_ptr<AbstractLqpNode>& lqp) {
  std::vector<std::shared_ptr<AbstractLqpNode>> nodes;
  VisitLqp(lqp, [&](const auto& node) {
    if (node->InputNodeCount() > 0) {
      return LqpVisitation::kVisitInputs;
    } else {
      nodes.emplace_back(node);
    }
    return LqpVisitation::kDoNotVisitInputs;
  });

  return nodes;
}

ExpressionUnorderedSet find_column_expressions(const AbstractLqpNode& lqp_node,
                                               const std::unordered_set<ColumnId>& column_ids) {
  DebugAssert(lqp_node.Type() == LqpNodeType::kStoredTable || lqp_node.Type() == LqpNodeType::kMock,
              "Did not expect other node types than StoredTableNode, StaticTableNode and MockNode.");
  DebugAssert(!lqp_node.LeftInput(), "Only valid for data source nodes");

  const auto& output_expressions = lqp_node.OutputExpressions();
  auto column_expressions = ExpressionUnorderedSet{};
  column_expressions.reserve(column_ids.size());

  for (const auto& output_expression : output_expressions) {
    const auto column_expression = std::dynamic_pointer_cast<LqpColumnExpression>(output_expression);
    // TODO(julianmenzler): C++20: Replace with .contains
    if (column_expression && column_ids.find(column_expression->original_column_id_) != column_ids.end() &&
        *column_expression->original_node_.lock() == lqp_node) {
      [[maybe_unused]] const auto [_, success] = column_expressions.emplace(column_expression);
      DebugAssert(success, "Did not expect multiple column expressions for the same column id.");
    }
  }

  return column_expressions;
}

bool contains_matching_unique_constraint(const std::shared_ptr<LqpUniqueConstraints>& unique_constraints,
                                         const ExpressionUnorderedSet& expressions) {
  DebugAssert(!unique_constraints->empty(), "Invalid input: Set of unique constraints should not be empty.");
  DebugAssert(!expressions.empty(), "Invalid input: Set of expressions should not be empty.");

  // Look for a unique constraint that is based on a subset of the given expressions
  for (const auto& unique_constraint : *unique_constraints) {
    if (unique_constraint.expressions.size() <= expressions.size() &&
        std::all_of(unique_constraint.expressions.cbegin(), unique_constraint.expressions.cend(),
                    [&expressions](const auto unique_constraint_expression) {
                      // TODO(julianmenzler): C++20: Replace with .contains
                      return expressions.find(unique_constraint_expression) != expressions.end();
                    })) {
      // Found a matching unique constraint
      return true;
    }
  }
  // Did not find a unique constraint for the given expressions
  return false;
}

std::vector<FunctionalDependency> fds_from_unique_constraints(
    const std::shared_ptr<const AbstractLqpNode>& lqp,
    const std::shared_ptr<LqpUniqueConstraints>& unique_constraints) {
  Assert(!unique_constraints->empty(), "Did not expect empty vector of unique constraints");

  auto fds = std::vector<FunctionalDependency>{};

  // Collect non-nullable output expressions
  const auto& output_expressions = lqp->OutputExpressions();
  auto output_expressions_non_nullable = ExpressionUnorderedSet{};
  for (auto column_id = ColumnId{0}; column_id < output_expressions.size(); ++column_id) {
    if (!lqp->IsColumnNullable(column_id)) {
      output_expressions_non_nullable.insert(output_expressions.at(column_id));
    }
  }

  for (const auto& unique_constraint : *unique_constraints) {
    auto determinants = unique_constraint.expressions;

    // (1) Verify whether we can create an FD from the given unique constraint (non-nullable determinant expressions)
    if (!std::all_of(determinants.cbegin(), determinants.cend(),
                     [&output_expressions_non_nullable](const auto& determinant_expression) {
                       // TODO(julianmenzler): C++20: Replace with .contains
                       return output_expressions_non_nullable.find(determinant_expression) !=
                              output_expressions_non_nullable.end();
                     })) {
      continue;
    }

    // (2) Collect the dependent output expressions
    auto dependents = ExpressionUnorderedSet();
    for (const auto& output_expression : output_expressions) {
      // TODO(julianmenzler): C++20: Replace with .contains
      if (determinants.find(output_expression) != determinants.end()) continue;
      dependents.insert(output_expression);
    }

    // (3) Add FD to output
    if (dependents.empty()) continue;
    DebugAssert(std::find_if(fds.cbegin(), fds.cend(),
                             [&determinants, &dependents](const auto& fd) {
                               return (fd.determinants == determinants) && (fd.dependents == dependents);
                             }) == fds.cend(),
                "Creating duplicate functional dependencies is unexpected.");
    fds.emplace_back(determinants, dependents);
  }
  return fds;
}

void remove_invalid_fds(const std::shared_ptr<const AbstractLqpNode>& lqp, std::vector<FunctionalDependency>& fds) {
  if (fds.empty()) return;
  const auto& output_expressions = lqp->OutputExpressions();
  const auto& output_expressions_set = ExpressionUnorderedSet{output_expressions.cbegin(), output_expressions.cend()};

  // Adjust FDs: Remove dependents that are not part of the node's output expressions
  auto not_part_of_output_expressions = [&output_expressions_set](const auto& fd_dependent_expression) {
    // TODO(julianmenzler): C++20: Replace with .contains
    return output_expressions_set.find(fd_dependent_expression) == output_expressions_set.end();
  };
  for (auto& fd : fds) {
    // TODO(julianmenzler): C++20: Replace with std::erase_if
    for (auto dependent = fd.dependents.begin(), last_dependent = fd.dependents.end(); dependent != last_dependent;) {
      if (not_part_of_output_expressions(*dependent)) {
        dependent = fd.dependents.erase(dependent);
      } else {
        ++dependent;
      }
    }
  }

  // Remove invalid or unnecessary FDs
  fds.erase(
      std::remove_if(fds.begin(), fds.end(),
                     [&lqp, &output_expressions_set](auto& fd) {
                       // If there are no dependents left, we can discard the FD altogether
                       if (fd.dependents.empty()) return true;

                       /**
                        * Remove FDs with determinant expressions that are
                        *  a) not part of the node's output expressions
                        *  b) are nullable
                        */
                       for (const auto& fd_determinant_expression : fd.determinants) {
                         // TODO(julianmenzler): C++20: Replace with .contains
                         if (output_expressions_set.find(fd_determinant_expression) == output_expressions_set.end() ||
                             lqp->IsColumnNullable(lqp->GetColumnId(*fd_determinant_expression))) {
                           return true;
                         }
                       }
                       return false;
                     }),
      fds.end());

  /**
   * Future Work: Remove redundant FDs. For example:
   *               - {a, b} => {c, SUM(d)}
   *               - {a}    => {b, c}
   *              Because we already have {a} => {c}, we do not need {a, b} => {c}. Therefore, we should change our set
   *              of FDs to the following:
   *               - {a, b} => {SUM(d)}
   *               - {a}    => {b, c}
   */
}

}  // namespace skyrise
