#include "abstract_chunk_writer.hpp"

namespace skyrise {

void AbstractFormatWriter::WriteToOutput(const char* data, size_t length) {
  if (callback_) {
    callback_(data, length);
  }
}

void AbstractFormatWriter::SetOutputHandler(std::function<void(const char* data, size_t length)> callback) {
  callback_ = std::move(callback);
}

}  // namespace skyrise
