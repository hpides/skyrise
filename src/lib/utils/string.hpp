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
inline const std::string kCharacterSetUpper = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
inline const std::string kCharacterSetLower = "abcdefghijklmnopqrstuvwxyz";
inline const std::string kCharacterSetDecimal = "0123456789";
inline const std::string kCharacterSetHex = "0123456789abcdef";

/**
 * Crops @param file_path to ensure readable Assert messages.
 * E.g., "/long/path/1234/src/lib/file.cpp" becomes "src/lib/file.cpp"
 */
std::string TrimSourceFilePath(const std::string& file_path);

/**
 * @return a vector of substrings from @param string using @param delimiter.
 */
std::vector<std::string> SplitStringByDelimiter(const std::string& string, const char delimiter);

/**
 * Converts the given @param stream to a string.
 */
template <typename T>
std::string StreamToString(T* stream) {
  std::ostringstream string_stream;
  string_stream << stream->rdbuf();
  stream->seekg(std::ios::beg);

  return string_stream.str();
}

/**
 * Converts @param vector to a string using @param delimiter.
 */
template <typename T>
std::string VectorToString(const std::vector<T>& vector, const std::string& delimiter) {
  std::ostringstream string_stream;

  if (!vector.empty()) {
    std::copy(vector.cbegin(), vector.cend() - 1, std::ostream_iterator<T>(string_stream, delimiter.c_str()));

    string_stream << vector.back();
  }

  return string_stream.str();
}

/**
 * @return A randomly generated string.
 */
// TODO(tobodner): Fix via std::format.
// NOLINTNEXTLINE(performance-inefficient-string-concatenation)
std::string RandomString(const size_t length, const std::string& character_set = kCharacterSetUpper +
                                                                                 kCharacterSetLower +
                                                                                 kCharacterSetDecimal);

/**
 * @return The passed string is prefixed with a timestamp and suffixed with an 8 digit hash.
 */
std::string GetUniqueName(const std::string& name);

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

/**
 * @return The passed string in lowercase characters.
 */
std::string ToLowerCase(const std::string& input_string);

}  // namespace skyrise
