#pragma once

#include <cstdint>
#include <cstdlib>
#include <optional>
#include <vector>

namespace skyrise {

class ByteBuffer {
 public:
  explicit ByteBuffer(size_t initial_capacity = 0);
  ByteBuffer(void* memory, size_t length);

  uint8_t* Data();
  char* CharData();
  size_t Size();
  void Resize(size_t new_size);

 private:
  void UseExternalDataAgain(size_t new_size);

  uint8_t* external_data_;
  size_t external_data_capacity_;
  size_t external_data_size_;
  std::optional<std::vector<uint8_t>> internal_data_;
};

}  // namespace skyrise
