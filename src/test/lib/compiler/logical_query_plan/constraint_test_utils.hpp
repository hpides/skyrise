/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#pragma once

#include "expression/lqp_column_expression.hpp"
#include "storage/table/table_key_constraint.hpp"

namespace skyrise {

/**
 * Verifies whether a given table key constraint is represented in a given set of unique constraints.
 */
static bool find_unique_constraint_by_key_constraint(const TableKeyConstraint& table_key_constraint,
                                                     const std::shared_ptr<LqpUniqueConstraints>& unique_constraints) {
  const auto& column_ids = table_key_constraint.columns();

  for (const auto& unique_constraint : *unique_constraints) {
    // Basic comparison: Column count
    if (column_ids.size() != unique_constraint.expressions.size()) {
      continue;
    }

    // In-depth comparison: Column IDs
    auto unique_constraint_column_ids = std::unordered_set<ColumnId>();
    for (const auto& expression : unique_constraint.expressions) {
      const auto& column_expression = std::dynamic_pointer_cast<LqpColumnExpression>(expression);
      if (column_expression) {
        unique_constraint_column_ids.emplace(column_expression->original_column_id_);
      }
    }

    if (unique_constraint_column_ids == column_ids) {
      return true;
    }
  }

  // Did not find a matching unique constraint
  return false;
}

}  // namespace skyrise
