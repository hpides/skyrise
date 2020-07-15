/**
 * Taken and modified from our Hyrise sister project (https://github.com/hyrise/hyrise at commit b856b57)
 *
 * Changelog:
 * - Change namespace
 * - Remove methods plugin_name_from_path, replace_addresses, split_string_by_delimiter, trim_and_split
 */

#pragma once

#include <string>

namespace skyrise {

// Since CI pathes of source files can be quite long AND we want Assert-messages to be readable, we crop
// "/long/very/long/path/1234/src/lib/file.cpp" to "src/lib/file.cpp"
std::string trim_source_file_path(const std::string& path);

}  // namespace skyrise
