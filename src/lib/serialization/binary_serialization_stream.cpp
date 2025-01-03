#include "binary_serialization_stream.hpp"

namespace skyrise {

#if __BYTE_ORDER != __LITTLE_ENDIAN
#error "Big Endian support is not implemented yet."
#endif

BinarySerializationStream::BinarySerializationStream(std::shared_ptr<std::iostream> io) : io_(std::move(io)) {}

bool BinarySerializationStream::Good() { return io_->good(); }

BinarySerializationStream& BinarySerializationStream::operator<<(int64_t value) {
  io_->write(reinterpret_cast<char*>(&value), sizeof(int64_t));
  return *this;
}

BinarySerializationStream& BinarySerializationStream::operator>>(int64_t& value) {
  io_->read(reinterpret_cast<char*>(&value), sizeof(value));
  return *this;
}

BinarySerializationStream& BinarySerializationStream::operator<<(bool value) {
  int8_t numeric_bool = value ? 1 : 0;
  io_->write(reinterpret_cast<char*>(&numeric_bool), sizeof(int8_t));
  return *this;
}

BinarySerializationStream& BinarySerializationStream::operator>>(bool& value) {
  int8_t numeric_bool = 0;
  io_->read(reinterpret_cast<char*>(&numeric_bool), sizeof(int8_t));
  value = (numeric_bool == 1);
  return *this;
}

BinarySerializationStream& BinarySerializationStream::operator<<(const std::string& value) {
  auto string_length = static_cast<int64_t>(value.size());
  *this << string_length;
  io_->write(value.c_str(), string_length);
  return *this;
}
BinarySerializationStream& BinarySerializationStream::operator>>(std::string& value) {
  int64_t string_length = 0;
  *this >> string_length;
  value.resize(string_length);
  io_->read(value.data(), string_length);
  return *this;
}

}  // namespace skyrise
