/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#pragma once

#include <optional>
#include <string>

namespace skyrise {

struct SqlIdentifier final {
  SqlIdentifier(const std::string& init_column_name, const std::optional<std::string>& init_table_name =
                                                         std::nullopt);  // NOLINT - Implicit conversion is intended

  bool operator==(const SqlIdentifier& rhs) const;

  std::string AsString() const;

  std::string column_name;
  std::optional<std::string> table_name = std::nullopt;
};

}  // namespace skyrise
