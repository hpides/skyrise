#pragma once

#include <string>

#include "storage/table/table_column_definition.hpp"
#include "storage/table/table_key_constraint.hpp"
#include "types.hpp"

namespace skyrise {

class TableSchema : Noncopyable {
 public:
  explicit TableSchema(const TableColumnDefinitions& column_definitions);

  ColumnId ColumnIdByName(const std::string& column_name) const;
  const std::string& ColumnName(const ColumnId column_id) const;

  DataType ColumnDataType(const ColumnId column_id) const;
  bool ColumnIsNullable(const ColumnId column_id) const;

  TableColumnDefinition GetTableColumnDefinition(const ColumnId column_id) const;

  ColumnCount TableColumnCount() const;

  void AddKeyConstraint(const TableKeyConstraint& table_key_constraint);
  const TableKeyConstraints& KeyConstraints() const;

  static std::shared_ptr<TableSchema> FromTableColumnDefinitions(const TableColumnDefinitions& column_definitions);

 private:
  const TableColumnDefinitions column_definitions_;
  TableKeyConstraints table_key_constraints_;
};

}  // namespace skyrise
