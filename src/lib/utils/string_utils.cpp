/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */

#include "string_utils.hpp"

#include <string>

namespace skyrise {

std::string TrimSourceFilePath(const std::string& path) {
  const auto src_position = path.find("/src/");

  return src_position == std::string::npos ? path : path.substr(src_position + 1);
}

}  // namespace skyrise
