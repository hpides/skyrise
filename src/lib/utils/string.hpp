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

}  // namespace skyrise
