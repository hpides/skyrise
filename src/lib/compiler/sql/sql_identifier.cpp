/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#include "sql_identifier.hpp"

#include <sstream>

namespace skyrise {

SqlIdentifier::SqlIdentifier(const std::string& init_column_name, const std::optional<std::string>& init_table_name)
    : column_name(init_column_name), table_name(init_table_name) {}

bool SqlIdentifier::operator==(const SqlIdentifier& rhs) const {
  return column_name == rhs.column_name && table_name == rhs.table_name;
}

std::string SqlIdentifier::AsString() const {
  std::stringstream ss;
  if (table_name) ss << *table_name << ".";
  ss << column_name;
  return ss.str();
}

}  // namespace skyrise
