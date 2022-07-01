#include "table_schema.hpp"

#include <algorithm>

#include "utils/assert.hpp"

namespace skyrise {

TableSchema::TableSchema(const TableColumnDefinitions& column_definitions) : column_definitions_(column_definitions) {
  Assert(!column_definitions.empty(), "At least one column definition is required.");
}

ColumnId TableSchema::ColumnIdByName(const std::string& column_name) const {
  const auto iter = std::find_if(column_definitions_.begin(), column_definitions_.end(),
                                 [&](const auto& column_definition) { return column_definition.name == column_name; });
  Assert(iter != column_definitions_.end(), "Couldn't find column '" + column_name + "'.");
  return static_cast<ColumnId>(std::distance(column_definitions_.begin(), iter));
}

const std::string& TableSchema::ColumnName(const ColumnId column_id) const {
  Assert(column_id < column_definitions_.size(), "ColumnId out of range.");
  return column_definitions_[column_id].name;
}

DataType TableSchema::ColumnDataType(const ColumnId column_id) const {
  Assert(column_id < column_definitions_.size(), "ColumnId out of range.");
  return column_definitions_[column_id].data_type;
}

bool TableSchema::ColumnIsNullable(const ColumnId column_id) const {
  Assert(column_id < column_definitions_.size(), "ColumnId out of range.");
  return column_definitions_[column_id].nullable;
}

TableColumnDefinition TableSchema::GetTableColumnDefinition(const ColumnId column_id) const {
  Assert(column_id < column_definitions_.size(), "ColumnId out of range.");
  return column_definitions_[column_id];
}

ColumnCount TableSchema::TableColumnCount() const { return static_cast<ColumnCount>(column_definitions_.size()); }

std::shared_ptr<TableSchema> TableSchema::FromTableColumnDefinitions(const TableColumnDefinitions& column_definitions) {
  return std::make_shared<TableSchema>(column_definitions);
}

void TableSchema::AddKeyConstraint(const TableKeyConstraint& table_key_constraint) {
  // Check validity of specified columns
  for (const auto& column_id : table_key_constraint.ColumnIds()) {
    Assert(column_id < TableColumnCount(), "ColumnId out of range.");

    // PRIMARY KEY requires non-nullable columns
    if (table_key_constraint.KeyType() == KeyConstraintType::kPrimaryKey) {
      Assert(!ColumnIsNullable(column_id), "Column must be non-nullable to comply with PRIMARY KEY.");
    }
  }

  for (const auto& existing_constraint : table_key_constraints_) {
    // Ensure that no other PRIMARY KEY is defined
    Assert(existing_constraint.KeyType() == KeyConstraintType::kUnique ||
               table_key_constraint.KeyType() == KeyConstraintType::kUnique,
           "Another primary key already exists for this table.");

    // Ensure there is only one key constraint per column set.
    Assert(existing_constraint.ColumnIds() != table_key_constraint.ColumnIds(),
           "Another key constraint for the same column set has already been defined.");
  }

  table_key_constraints_.push_back(table_key_constraint);
}

const TableKeyConstraints& TableSchema::KeyConstraints() const { return table_key_constraints_; }

}  // namespace skyrise
