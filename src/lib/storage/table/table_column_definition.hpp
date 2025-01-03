/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#pragma once

#include <iostream>
#include <string>
#include <vector>

#include "all_type_variant.hpp"

namespace skyrise {

struct TableColumnDefinition final {
  TableColumnDefinition() = default;
  TableColumnDefinition(std::string name, DataType data_type, bool nullable);

  bool operator==(const TableColumnDefinition& rhs) const;

  std::string name;
  DataType data_type{DataType::kInt};
  bool nullable{false};
};

// So that GoogleTest, e.g., prints readable error messages
inline std::ostream& operator<<(std::ostream& stream, const TableColumnDefinition& definition) {
  stream << definition.name << " " << definition.data_type << " "
         << (definition.nullable ? "nullable" : "not nullable");
  return stream;
}

using TableColumnDefinitions = std::vector<TableColumnDefinition>;

TableColumnDefinitions Concatenated(const TableColumnDefinitions& lhs, const TableColumnDefinitions& rhs);

}  // namespace skyrise
