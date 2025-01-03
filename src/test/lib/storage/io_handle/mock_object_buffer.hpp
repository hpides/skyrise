#pragma once

#include "storage/backend/abstract_storage.hpp"
#include "storage/backend/testdata_storage.hpp"
#include "storage/io_handle/object_buffer.hpp"
namespace skyrise {

class MockObjectBuffer : public ObjectBuffer {
 public:
  explicit MockObjectBuffer(const std::string& data_path,
                            std::shared_ptr<Storage> storage = std::make_shared<TestdataStorage>());

  size_t BufferSize() const;

 private:
  size_t data_size_ = 0;
  std::shared_ptr<Storage> storage_;
};

}  // namespace skyrise
