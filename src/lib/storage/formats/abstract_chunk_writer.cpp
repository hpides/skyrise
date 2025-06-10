#include "abstract_chunk_writer.hpp"

namespace skyrise {

void AbstractFormatWriter::WriteToOutput(const char* data, size_t length) {
  if (callback_) {
    bytes_written_ += length;
    callback_(data, length);
  }
}

void AbstractFormatWriter::SetOutputHandler(std::function<void(const char* data, size_t length)> callback) {
  callback_ = std::move(callback);
}

size_t AbstractFormatWriter::GetNumberOfWrittenBytes() const { return bytes_written_; }

}  // namespace skyrise
