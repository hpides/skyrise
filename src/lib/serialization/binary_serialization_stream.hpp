#pragma once

#include <iostream>
#include <memory>

namespace skyrise {

class BinarySerializationStream {
 public:
  explicit BinarySerializationStream(std::shared_ptr<std::iostream> io);

  BinarySerializationStream& operator<<(int64_t value);
  BinarySerializationStream& operator>>(int64_t& value);

  BinarySerializationStream& operator<<(bool value);
  BinarySerializationStream& operator>>(bool& value);

  BinarySerializationStream& operator<<(const std::string& value);
  BinarySerializationStream& operator>>(std::string& value);

  bool Good();

 private:
  std::shared_ptr<std::iostream> io_;
};

}  // namespace skyrise
