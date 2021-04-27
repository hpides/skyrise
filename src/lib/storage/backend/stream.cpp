#include "stream.hpp"

#include <aws/core/utils/logging/LogMacros.h>

namespace skyrise {

ObjectReaderStreamBuffer::ObjectReaderStreamBuffer(std::unique_ptr<ObjectReader> reader) : reader_(std::move(reader)) {
  buffer_.reserve(kBufferSize);
  setg(nullptr, nullptr, nullptr);  // We need to fill the buffer.
}

// Implements lazy lookup for object size.
size_t ObjectReaderStreamBuffer::GetObjectSize() {
  if (!object_size_.has_value()) {
    const ObjectStatus& status = reader_->GetStatus();
    if (status.GetError()) {
      throw std::logic_error("Could not get status of object");  // Signal failure to std::istream.
    }
    *object_size_ = status.GetSize();
  }

  return *object_size_;
}

ObjectReaderStreamBuffer::pos_type ObjectReaderStreamBuffer::seekpos(pos_type pos,
                                                                     [[maybe_unused]] std::ios_base::openmode which) {
  const auto absolute_position = static_cast<size_t>(pos);
  if (absolute_position > GetObjectSize()) {
    return pos_type(off_type(-1));
  }

  if (absolute_position >= current_offset_ && absolute_position < current_offset_ + buffer_.size()) {
    // If we can serve this new position from our buffer, just reposition the get pointer.
    setg(buffer_.data(), buffer_.data() + (absolute_position - current_offset_), buffer_.data() + buffer_.size());
  } else {
    // Otherwise prepare call to underflow().
    current_offset_ = absolute_position;
    setg(nullptr, nullptr, nullptr);
  }

  return pos;
}

ObjectReaderStreamBuffer::pos_type ObjectReaderStreamBuffer::seekoff(off_type off, std::ios_base::seekdir dir,
                                                                     [[maybe_unused]] std::ios_base::openmode which) {
  switch (dir) {
    case std::ios_base::beg:
      // Treat `off` as absolute offset.
      return seekpos(pos_type(off));
    case std::ios_base::cur:
      // If we do not have a valid buffer we just add `off` to the current offset, which will be used to fetch the next
      // buffer. Otherwise we calculate the absolute offset of the current get pointer moved by `off`. Seekpos will
      // check, if this can still be served with the current buffer.
      return seekpos(pos_type(gptr() == nullptr ? current_offset_ + off : current_offset_ + (gptr() - eback() + off)));
    case std::ios_base::end:
      // Here `off` should be negative and be the absolute offset by adding it to the object size.
      return seekpos(pos_type(GetObjectSize() + off));
    default:
      return pos_type(off_type(-1));
  }
}

int ObjectReaderStreamBuffer::underflow() {
  if (gptr() != nullptr) {
    // If we consumed a buffer, we will load the next.
    current_offset_ += buffer_.size();
  }

  if (current_offset_ >= GetObjectSize()) {
    return traits_type::eof();
  }

  buffer_.clear();

  size_t read_from = current_offset_;
  size_t read_until_inclusive = std::min(GetObjectSize(), current_offset_ + kBufferSize) - 1;
  size_t expected_bytes = read_until_inclusive - read_from + 1;

  if (expected_bytes == 0) {
    return traits_type::eof();
  }

  StorageError read_result = reader_->Read(read_from, read_until_inclusive, [this](const char* data, size_t length) {
    buffer_.insert(buffer_.end(), data, data + length);
  });

  if (read_result.IsError()) {
    AWS_LOGSTREAM_ERROR(kStreamLoggingTag, "Read failed with message: " << read_result.GetMessage());
    throw std::logic_error("Error while reading from object");  // Signal failure to std::istream.
  }

  const size_t actually_read_bytes = buffer_.size();
  if (actually_read_bytes != expected_bytes) {
    AWS_LOGSTREAM_ERROR(kStreamLoggingTag, "Read unexpected number of bytes. Maybe the object has changed?");
    throw std::logic_error("Unexpected number of bytes");  // Signal failure to std::istream.
  }

  setg(buffer_.data(), buffer_.data(), buffer_.data() + actually_read_bytes);
  return traits_type::to_int_type(*gptr());
}

ObjectReaderStream::ObjectReaderStream(std::unique_ptr<ObjectReader> reader)
    : std::iostream(&stream_buffer_), stream_buffer_(std::move(reader)) {}

}  // namespace skyrise
