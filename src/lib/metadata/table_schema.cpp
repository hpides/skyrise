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
  DebugAssert(column_id < column_definitions_.size(), "ColumnId out of range.");
  return column_definitions_[column_id].name;
}

DataType TableSchema::ColumnDataType(const ColumnId column_id) const {
  DebugAssert(column_id < column_definitions_.size(), "ColumnId out of range.");
  return column_definitions_[column_id].data_type;
}

bool TableSchema::ColumnIsNullable(const ColumnId column_id) const {
  DebugAssert(column_id < column_definitions_.size(), "ColumnId out of range.");
  return column_definitions_[column_id].nullable;
}

TableColumnDefinition TableSchema::GetTableColumnDefinition(const ColumnId column_id) const {
  DebugAssert(column_id < column_definitions_.size(), "ColumnId out of range.");
  return column_definitions_[column_id];
}

ColumnCount TableSchema::TableColumnCount() const { return static_cast<ColumnCount>(column_definitions_.size()); }

}  // namespace skyrise
