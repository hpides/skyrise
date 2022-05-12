/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#pragma once

#include <unordered_set>

#include "types.hpp"

namespace skyrise {

/**
 * Abstract container class for the definition of table constraints based on a set of column ids.
 * Subclasses should leverage the OOP structure to add additional fields. In case of CHECK and FOREIGN KEY constraint
 * implementations, these fields may include check definitions and referenced keys.
 */
class AbstractTableConstraint {
 public:
  explicit AbstractTableConstraint(std::unordered_set<ColumnId> columns);

  AbstractTableConstraint(const AbstractTableConstraint&) = default;
  AbstractTableConstraint(AbstractTableConstraint&&) = default;
  AbstractTableConstraint& operator=(const AbstractTableConstraint&) = default;
  AbstractTableConstraint& operator=(AbstractTableConstraint&&) = default;

  virtual ~AbstractTableConstraint() = default;

  const std::unordered_set<ColumnId>& ColumnIds() const;

  bool operator==(const AbstractTableConstraint& rhs) const;
  bool operator!=(const AbstractTableConstraint& rhs) const;

 protected:
  /**
   * Compare two table constraints of the same type. Only additional fields have to be compared since column ids are
   * already compared by the caller.
   */
  virtual bool OnEquals(const AbstractTableConstraint& table_constraint) const = 0;

 private:
  std::unordered_set<ColumnId> columns_;
};

}  // namespace skyrise
