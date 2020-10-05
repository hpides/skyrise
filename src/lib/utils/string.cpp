/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */

#include "string.hpp"

#include <memory>
#include <string>

#include <aws/core/Aws.h>

namespace skyrise {

std::string TrimSourceFilePath(const std::string& path) {
  const auto src_position = path.find("/src/");

  return src_position == std::string::npos ? path : path.substr(src_position + 1);
}

Aws::String StreamToString(Aws::IOStream* stream) {
  Aws::StringStream string_stream;
  string_stream << stream->rdbuf();
  stream->seekg(std::ios::beg);

  return string_stream.str();
}

}  // namespace skyrise
