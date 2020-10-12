/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */

#pragma once

#include <memory>
#include <string>

#include <aws/core/Aws.h>

namespace skyrise {

// Crop a source file path to ensure readable assert messages (e.g., "/long/path/1234/src/lib/file.cpp" becomes
// "src/lib/file.cpp")
std::string TrimSourceFilePath(const std::string& path);

// Convert a stream to a string
Aws::String StreamToString(Aws::IOStream* stream);

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
