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

size_t StringHeapSize(const std::string& string) {
  // Get the default pre-allocated capacity of SSO strings. Note that the empty string has an unspecified capacity, so
  // we use a really short one here.
  size_t sso_string_capacity = std::string{"."}.capacity();

  if (string.capacity() > sso_string_capacity) {
    // For heap-allocated strings, \0 is appended to denote the end of the string. capacity() is used over length()
    // since some libraries (e.g. llvm's libc++) also over-allocate the heap strings
    // (cf. https://shaharmike.com/cpp/std-string/).
    return string.capacity() + 1;
  }

  // Assert that SSO meets expectations
  assert(string.capacity() == sso_string_capacity);
  return 0;
}

size_t StringVectorMemoryUsage(const std::vector<std::string>& string_vector) {
  size_t base_size = sizeof(std::vector<std::string>);

  // Early out
  if (string_vector.empty()) {
    return base_size + (string_vector.capacity() * sizeof(std::string));
  }

  // Run the (expensive) calculation of aggregating the whole vector's string sizes when full estimation is desired
  // or the given input vector is small.
  size_t elements_size = string_vector.capacity() * sizeof(std::string);
  for (const auto& single_string : string_vector) {
    elements_size += StringHeapSize(single_string);
  }
  return base_size + elements_size;
}

}  // namespace skyrise
