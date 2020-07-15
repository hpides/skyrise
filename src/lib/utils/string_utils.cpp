/**
 * Taken and modified from our Hyrise sister project
 *
 * See header for changelog
 */

#include "string_utils.hpp"

namespace skyrise {

std::string trim_source_file_path(const std::string& path) {
  const auto src_pos = path.find("/src/");
  if (src_pos == std::string::npos) return path;

  // "+ 1", since we want "src/lib/file.cpp" and not "/src/lib/file.cpp"
  return path.substr(src_pos + 1);
}

}  // namespace skyrise
