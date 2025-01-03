#include "mock_object_buffer.hpp"

#include "storage/backend/testdata_storage.hpp"
#include "utils/assert.hpp"

namespace skyrise {

MockObjectBuffer::MockObjectBuffer(const std::string& data_path, std::shared_ptr<Storage> storage)
    : storage_(std::move(storage)) {
  const auto reader = storage_->OpenForReading(data_path);
  auto byte_buffer = std::make_shared<ByteBuffer>();
  data_size_ = reader->GetStatus().GetSize();
  const StorageError storage_error = reader->Read(0, data_size_, byte_buffer.get());
  Assert(storage_error.GetType() != StorageErrorType::kNotFound, "File not found.");
  Assert(!storage_error.IsError(), storage_error.GetMessage());
  AddBuffer({{0, data_size_}, byte_buffer});
}

size_t MockObjectBuffer::BufferSize() const { return data_size_; }

}  // namespace skyrise
