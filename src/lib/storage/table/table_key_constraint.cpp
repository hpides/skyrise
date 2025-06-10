/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#include "table_key_constraint.hpp"

#include "utils/assert.hpp"

namespace skyrise {

TableKeyConstraint::TableKeyConstraint(std::unordered_set<ColumnId> columns, KeyConstraintType key_type)
    : AbstractTableConstraint(std::move(columns)), key_type_(key_type) {}

KeyConstraintType TableKeyConstraint::KeyType() const { return key_type_; }

bool TableKeyConstraint::OnEquals(const AbstractTableConstraint& table_constraint) const {
  DebugAssert(dynamic_cast<const TableKeyConstraint*>(&table_constraint),
              "Different table_constraint type should have been caught by AbstractTableConstraint::operator==.");
  return KeyType() == dynamic_cast<const TableKeyConstraint&>(table_constraint).KeyType();
}

}  // namespace skyrise
