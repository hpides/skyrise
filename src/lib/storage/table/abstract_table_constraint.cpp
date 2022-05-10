/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#include "abstract_table_constraint.hpp"

namespace skyrise {

AbstractTableConstraint::AbstractTableConstraint(std::unordered_set<ColumnId> columns) : columns_(std::move(columns)) {}

const std::unordered_set<ColumnId>& AbstractTableConstraint::ColumnIds() const { return columns_; }

bool AbstractTableConstraint::operator==(const AbstractTableConstraint& rhs) const {
  if (this == &rhs) {
    return true;
  }
  if (typeid(*this) != typeid(rhs)) {
    return false;
  }
  if (ColumnIds() != rhs.ColumnIds()) {
    return false;
  }
  return OnEquals(rhs);
}

bool AbstractTableConstraint::operator!=(const AbstractTableConstraint& rhs) const { return !(rhs == *this); }

}  // namespace skyrise
