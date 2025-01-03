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
    return {static_cast<off_type>(-1)};
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
      // buffer. Otherwise, we calculate the absolute offset of the current get pointer moved by `off`. Seekpos will
      // check, if this can still be served with the current buffer.
      return seekpos(pos_type(gptr() == nullptr ? current_offset_ + off : current_offset_ + (gptr() - eback() + off)));
    case std::ios_base::end:
      // Here `off` should be negative and be the absolute offset by adding it to the object size.
      return seekpos(pos_type(GetObjectSize() + off));
    default:
      return {static_cast<off_type>(-1)};
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

  const size_t read_from = current_offset_;
  const size_t read_until_inclusive = std::min(GetObjectSize(), current_offset_ + kBufferSize) - 1;
  const size_t expected_bytes = read_until_inclusive - read_from + 1;

  if (expected_bytes == 0) {
    return traits_type::eof();
  }

  buffer_.resize(expected_bytes);
  ByteBuffer buffer_view(buffer_.data(), expected_bytes);
  const StorageError read_result = reader_->Read(read_from, read_until_inclusive, &buffer_view);
  buffer_.resize(buffer_view.Size());

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

void ObjectReaderStreamBuffer::FillBufferWithTail() {
  buffer_.clear();

  ByteBuffer buffer_view(buffer_.data(), kBufferSize);
  const StorageError readtail_result = reader_->ReadTail(kBufferSize, &buffer_view);
  buffer_.resize(buffer_view.Size());

  if (readtail_result.IsError()) {
    // Since this is an optimization, we do not propagate the error. Instead, a subsequent read will.
    AWS_LOGSTREAM_ERROR(kStreamLoggingTag, "Read failed with message: " << readtail_result.GetMessage());
    return;
  }

  const size_t actually_read_bytes = buffer_.size();
  setg(buffer_.data(), buffer_.data(), buffer_.data() + actually_read_bytes);
}

ObjectReaderStream::ObjectReaderStream(std::unique_ptr<ObjectReader> reader, bool initial_fill_buffer_with_tail)
    : std::iostream(&stream_buffer_), stream_buffer_(std::move(reader)) {
  if (initial_fill_buffer_with_tail) {
    stream_buffer_.FillBufferWithTail();
  }
}

DelegateStreamBuffer::DelegateStreamBuffer(ByteBuffer* buffer) { Reset(buffer); }

void DelegateStreamBuffer::Reset(ByteBuffer* buffer) {
  buffer_ = buffer;
  setg(buffer_->CharData(), buffer_->CharData(), buffer_->CharData() + buffer_->Size());
  setp(buffer_->CharData(), buffer_->CharData());
}

std::streamsize DelegateStreamBuffer::xsputn(const char* s, std::streamsize n) {
  const size_t write_offset = pptr() - buffer_->CharData();
  const size_t read_offset = gptr() - buffer_->CharData();
  const size_t resulting_write_offset = write_offset + n;

  if (resulting_write_offset > buffer_->Size()) {
    buffer_->Resize(resulting_write_offset);
  }

  std::copy_n(s, n, buffer_->CharData() + write_offset);

  setg(buffer_->CharData(), buffer_->CharData() + read_offset, buffer_->CharData() + buffer_->Size());
  setp(buffer_->CharData() + write_offset + n, buffer_->CharData() + buffer_->Size());

  return n;
}

int DelegateStreamBuffer::overflow(int ch) {
  if (ch != traits_type::eof()) {
    const char c = ch;
    xsputn(&c, 1);
  }

  return ch;
}

DelegateStreamBuffer::pos_type DelegateStreamBuffer::seekpos(pos_type pos, std::ios::openmode which) {
  const bool is_in = (std::ios::in & which) != 0;
  const bool is_out = (std::ios::out & which) != 0;
  const size_t new_position = pos;
  const size_t max_position = buffer_->Size();

  if (new_position > max_position) {
    return {off_type(-1)};
  }

  if (is_in) {
    setg(buffer_->CharData(), buffer_->CharData() + pos, buffer_->CharData() + max_position);
  }

  if (is_out) {
    setp(buffer_->CharData() + pos, buffer_->CharData() + max_position);
  }

  return {off_type(pos)};
}

DelegateStreamBuffer::pos_type DelegateStreamBuffer::seekoff(DelegateStreamBuffer::off_type off, std::ios::seekdir dir,
                                                             std::ios::openmode which) {
  if (dir == std::ios::beg) {
    return seekpos(off, which);
  }
  if (dir == std::ios::end) {
    return seekpos(buffer_->Size() - off, which);
  }

  const bool is_in = (std::ios::in & which) != 0;
  const bool is_out = (std::ios::out & which) != 0;
  pos_type result = {off_type(-1)};

  if (dir == std::ios::cur) {
    if (is_in) {
      result = seekpos(this->gptr() - buffer_->CharData() + off, std::ios::in);
    }
    if (is_out) {
      result = seekpos(this->pptr() - buffer_->CharData() + off, std::ios::out);
    }
  }
  return result;
}

}  // namespace skyrise
