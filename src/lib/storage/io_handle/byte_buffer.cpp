#include "byte_buffer.hpp"

#include <algorithm>

namespace skyrise {

ByteBuffer::ByteBuffer(void* memory, size_t length)
    : external_data_(reinterpret_cast<uint8_t*>(memory)), external_data_capacity_(length), external_data_size_(0) {}

ByteBuffer::ByteBuffer(size_t initial_capacity)
    : external_data_(nullptr), external_data_capacity_(0), external_data_size_(0) {
  internal_data_.emplace();
  if (initial_capacity > 0) {
    internal_data_->reserve(initial_capacity);
  }
}

char* ByteBuffer::CharData() { return reinterpret_cast<char*>(Data()); }

uint8_t* ByteBuffer::Data() {
  if (internal_data_) {
    return internal_data_->data();
  }

  return external_data_;
}

size_t ByteBuffer::Size() {
  if (internal_data_) {
    return internal_data_->size();
  }

  return external_data_size_;
}

void ByteBuffer::Resize(size_t new_size) {
  if (internal_data_) {
    if (external_data_ && external_data_capacity_ >= new_size) {
      UseExternalDataAgain(new_size);
    } else {
      if (new_size > internal_data_->capacity()) {
        internal_data_->reserve(std::max<size_t>(new_size, internal_data_->capacity() * 2));
      }
      internal_data_->resize(new_size);
    }

    return;
  }

  if (new_size > external_data_capacity_) {
    internal_data_.emplace();
    internal_data_->resize(new_size);
    std::copy_n(external_data_, external_data_size_, internal_data_->data());
  } else {
    external_data_size_ = new_size;
  }
}

void ByteBuffer::UseExternalDataAgain(size_t new_size) {
  external_data_size_ = new_size;
  if (new_size > 0) {
    std::copy_n(internal_data_->data(), new_size, external_data_);
  }
  internal_data_.reset();
}

}  // namespace skyrise
