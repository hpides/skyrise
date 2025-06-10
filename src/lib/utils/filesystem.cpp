#include "filesystem.hpp"

#include <fstream>

#include "assert.hpp"
#include "configuration.hpp"

namespace skyrise {

std::string ReadFileToString(const std::string& filename) {
  std::ifstream in_stream(filename.c_str(), std::ios::in | std::ios::binary);

  if (!in_stream) {
    Fail(filename + " could not be opened.");
  }

  std::string content;

  in_stream.seekg(0, std::ios::end);

  const size_t file_size = in_stream.tellg();

  if (file_size > kMaxFileSizeBytes) {
    Fail(filename + " is too large.");
  }

  if (file_size > 0) {
    content.resize(file_size);
    in_stream.seekg(0, std::ios::beg);
    in_stream.read(&content.front(), static_cast<int64_t>(content.size()));
  }

  in_stream.close();

  return content;
}

void WriteStringToFile(const std::string& content, const std::string& filename) {
  std::ofstream out_stream(filename.c_str(), std::ios::out | std::ios::app);

  if (!out_stream) {
    Fail(filename + " could not be opened.");
  }

  out_stream << content;
  out_stream.close();
}

}  // namespace skyrise
