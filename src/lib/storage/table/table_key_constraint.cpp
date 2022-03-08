/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#include "table_key_constraint.hpp"

#include "utils/assert.hpp"

namespace skyrise {

TableKeyConstraint::TableKeyConstraint(std::unordered_set<ColumnId> init_columns, KeyConstraintType init_key_type)
    : AbstractTableConstraint(std::move(init_columns)), key_type_(init_key_type) {}

KeyConstraintType TableKeyConstraint::key_type() const { return key_type_; }

bool TableKeyConstraint::_on_equals(const AbstractTableConstraint& table_constraint) const {
  DebugAssert(dynamic_cast<const TableKeyConstraint*>(&table_constraint),
              "Different table_constraint type should have been caught by AbstractTableConstraint::operator==");
  return key_type() == static_cast<const TableKeyConstraint&>(table_constraint).key_type();
}

}  // namespace skyrise
