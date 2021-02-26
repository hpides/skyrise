/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */

#pragma once

#include <algorithm>
#include <iterator>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include <aws/core/Aws.h>

namespace skyrise {

// Character sets for randomly generated strings
const std::string kCharacterSetUpper = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
const std::string kCharacterSetLower = "abcdefghijklmnopqrstuvwxyz";
const std::string kCharacterSetDecimal = "0123456789";
const std::string kCharacterSetHex = "0123456789abcdef";

// Crop a source file path to ensure readable assert messages (e.g., "/long/path/1234/src/lib/file.cpp" becomes
// "src/lib/file.cpp")
std::string TrimSourceFilePath(const std::string& path);

// Convert a stream to a string
template <typename T>
std::string StreamToString(T* stream) {
  std::ostringstream string_stream;
  string_stream << stream->rdbuf();
  stream->seekg(std::ios::beg);

  return string_stream.str();
}

// Convert a vector to a string
template <typename T>
std::string VectorToString(const std::vector<T>& vector, const std::string& deliminter) {
  std::ostringstream string_stream;

  if (!vector.empty()) {
    std::copy(vector.cbegin(), vector.cend() - 1, std::ostream_iterator<T>(string_stream, deliminter.c_str()));

    string_stream << vector.back();
  }

  return string_stream.str();
}

// Create a randomly generated string
std::string RandomString(const size_t length, const std::string& character_set = kCharacterSetUpper +
                                                                                 kCharacterSetLower +
                                                                                 kCharacterSetDecimal);

/**
 * Get the number of bytes that are allocated on the heap for the given string.
 */
size_t StringHeapSize(const std::string& string);

/**
 * This function iterates over the given string vector @param string_vector strings and sums up the memory usage. Due
 * to the small string optimization (SSO) in most current C++ libraries, each string has an initially allocated buffer
 * (e.g., 15 chars in GCC's libstdc++). If a string is larger, the string is allocated on the heap and the initial
 * string object stores a pointer to the actual string on the heap.
 *
 * Please note, that there are still differences between the stdlib's. Also the full size accumulation is not
 * guaranteed to be 100% accurate for all libraries.
 */
size_t StringVectorMemoryUsage(const std::vector<std::string>& string_vector);

}  // namespace skyrise
