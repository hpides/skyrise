/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#include "abstract_table_constraint.hpp"

namespace skyrise {

AbstractTableConstraint::AbstractTableConstraint(std::unordered_set<ColumnId> init_columns)
    : columns_(std::move(init_columns)) {}

const std::unordered_set<ColumnId>& AbstractTableConstraint::columns() const { return columns_; }

bool AbstractTableConstraint::operator==(const AbstractTableConstraint& rhs) const {
  if (this == &rhs) return true;
  if (typeid(*this) != typeid(rhs)) return false;
  if (columns() != rhs.columns()) return false;
  return _on_equals(rhs);
}

bool AbstractTableConstraint::operator!=(const AbstractTableConstraint& rhs) const { return !(rhs == *this); }

}  // namespace skyrise
