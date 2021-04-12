/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#include "table_column_definition.hpp"

namespace skyrise {

TableColumnDefinition::TableColumnDefinition(std::string init_name, DataType init_data_type, bool init_nullable)
    : name(std::move(init_name)), data_type(init_data_type), nullable(init_nullable) {}

bool TableColumnDefinition::operator==(const TableColumnDefinition& rhs) const {
  return name == rhs.name && data_type == rhs.data_type && nullable == rhs.nullable;
}

TableColumnDefinitions Concatenated(const TableColumnDefinitions& lhs, const TableColumnDefinitions& rhs) {
  auto column_definitions = lhs;
  column_definitions.insert(column_definitions.end(), rhs.begin(), rhs.end());
  return column_definitions;
}

}  // namespace skyrise
