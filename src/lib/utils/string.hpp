/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */

#pragma once

#include <sstream>
#include <string>

namespace skyrise {

// Crop a source file path to ensure readable assert messages (e.g., "/long/path/1234/src/lib/file.cpp" becomes
// "src/lib/file.cpp")
std::string TrimSourceFilePath(const std::string& path);

// Convert a formatted stream to a string
template <typename T>
std::string StreamToString(const T& stream) {
  std::stringstream string_stream;
  string_stream << stream;
  return string_stream.str();
}

}  // namespace skyrise
