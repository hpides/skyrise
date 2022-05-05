/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#include "abstract_table_constraint.hpp"

namespace skyrise {

AbstractTableConstraint::AbstractTableConstraint(const std::unordered_set<ColumnId> columns)
    : columns_(std::move(columns)) {}

const std::unordered_set<ColumnId>& AbstractTableConstraint::Columns() const { return columns_; }

bool AbstractTableConstraint::operator==(const AbstractTableConstraint& rhs) const {
  if (this == &rhs) {
    return true;
  }
  if (typeid(*this) != typeid(rhs)) {
    return false;
  }
  if (Columns() != rhs.Columns()) {
    return false;
  }
  return OnEquals(rhs);
}

bool AbstractTableConstraint::operator!=(const AbstractTableConstraint& rhs) const { return !(rhs == *this); }

}  // namespace skyrise
