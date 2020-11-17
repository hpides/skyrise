#include "abstract_formatter.hpp"

namespace skyrise {

void AbstractFormatter::WriteToOutput(const char* data, size_t length) {
  if (callback_) {
    callback_(data, length);
  } else if (stream_) {
    stream_->write(data, length);
  }
}

void AbstractFormatter::SetOutput(std::function<void(const char* data, size_t length)> callback) {
  callback_ = std::move(callback);
  stream_.reset();
}

void AbstractFormatter::SetOutput(std::shared_ptr<std::iostream> stream) {
  stream_ = std::move(stream);
  callback_ = nullptr;
}

}  // namespace skyrise
