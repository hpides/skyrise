#include "mock_storage.hpp"

#include <algorithm>
#include <vector>

namespace skyrise {

MockReader::MockReader(std::shared_ptr<std::string> data, std::string identifier)
    : data_(std::move(data)), identifier_(std::move(identifier)) {}

StorageError MockReader::Read(size_t first_byte, size_t last_byte, ByteBuffer* buffer) {
  num_reads_++;

  if (!data_) {
    return StorageError(StorageErrorType::kNotFound);
  }

  if (first_byte >= data_->size()) {
    return StorageError::Success();
  }

  if (last_byte >= data_->size()) {
    last_byte = data_->size() - 1;
  }

  const size_t length = last_byte - first_byte + 1;
  buffer->Resize(length);
  std::copy_n(&data_->c_str()[first_byte], length, buffer->Data());

  return StorageError::Success();
}

const ObjectStatus& MockReader::GetStatus() {
  if (!data_) {
    status_ = ObjectStatus(StorageError(StorageErrorType::kNotFound));
  } else {
    status_ = ObjectStatus(identifier_, 0, "dummyChecksum", data_->size());
  }

  return status_;
}

const size_t& MockReader::GetReadOperationCounter() const { return num_reads_; }

StorageError MockReader::Close() { return StorageError::Success(); }

MockWriter::MockWriter(std::string object_identifier,
                       std::function<void(std::string&& key, std::string&& value)> setter)
    : object_identifier_(std::move(object_identifier)), setter_(std::move(setter)) {}

StorageError MockWriter::Write(const char* data, size_t length) {
  if (object_identifier_.empty()) {
    return StorageError(StorageErrorType::kInternalError);
  }
  stream_.write(data, length);
  return StorageError::Success();
}

StorageError MockWriter::Close() {
  if (object_identifier_.empty()) {
    return StorageError(StorageErrorType::kInternalError);
  }

  setter_(std::move(object_identifier_), stream_.str());
  return StorageError::Success();
}

std::unique_ptr<ObjectWriter> MockStorage::OpenForWriting(const std::string& object_identifier) {
  if (simulate_write_error_) {
    simulate_error_after_--;
    if (simulate_error_after_ <= 0) {
      simulate_error_after_ = 0;
      return std::make_unique<MockWriter>();
    }
  }

  return std::make_unique<MockWriter>(object_identifier, [&](std::string&& key, std::string&& value) {
    const std::lock_guard guard(store_mutex_);
    store_.insert_or_assign(std::move(key), std::make_shared<std::string>(std::move(value)));
  });
}

std::unique_ptr<ObjectReader> MockStorage::OpenForReading(const std::string& object_identifier) {
  const std::lock_guard guard(store_mutex_);

  auto iterator = store_.find(object_identifier);
  if (iterator == store_.end()) {
    return std::make_unique<MockReader>();
  }

  return std::make_unique<MockReader>(iterator->second, object_identifier);
}

StorageError MockStorage::Delete(const std::string& object_identifier) {
  const std::lock_guard guard(store_mutex_);

  auto iterator = store_.find(object_identifier);
  if (iterator != store_.end()) {
    store_.erase(iterator);
  }

  return StorageError::Success();
}

std::pair<std::vector<ObjectStatus>, StorageError> MockStorage::List(const std::string& /*object_prefix*/) {
  return std::make_pair(std::vector<ObjectStatus>(), StorageError(StorageErrorType::kOperationNotSupported));
}

void MockStorage::SetSimulateWriteErrorAfter(int n) {
  simulate_error_after_ = n;
  simulate_write_error_ = true;
}

}  // namespace skyrise
