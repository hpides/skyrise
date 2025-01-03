/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#pragma once

#include <vector>

#include "abstract_table_constraint.hpp"

namespace skyrise {

enum class KeyConstraintType { kPrimaryKey, kUnique };

/**
 * Container class to define uniqueness constraints for tables.
 * As defined by SQL, two types of keys are supported: PRIMARY KEY and UNIQUE keys.
 */
class TableKeyConstraint final : public AbstractTableConstraint {
 public:
  TableKeyConstraint(std::unordered_set<ColumnId> columns, KeyConstraintType key_type);

  KeyConstraintType KeyType() const;

 protected:
  bool OnEquals(const AbstractTableConstraint& table_constraint) const override;

 private:
  KeyConstraintType key_type_;
};

using TableKeyConstraints = std::vector<TableKeyConstraint>;

}  // namespace skyrise
