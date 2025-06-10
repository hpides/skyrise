#pragma once

#include <atomic>
#include <map>
#include <mutex>
#include <sstream>
#include <string>

#include "storage/backend/abstract_storage.hpp"

namespace skyrise {

class MockWriter : public ObjectWriter {
 public:
  MockWriter() = default;
  MockWriter(std::string object_identifier, std::function<void(std::string&& key, std::string&& value)> setter);
  StorageError Write(const char* data, size_t length) override;
  StorageError Close() override;

 private:
  std::string object_identifier_;
  std::stringstream stream_;
  std::function<void(std::string key, std::string value)> setter_;
};

class MockReader : public ObjectReader {
 public:
  MockReader() = default;
  MockReader(std::shared_ptr<std::string> data, std::string identifier);
  StorageError Read(size_t first_byte, size_t last_byte, ByteBuffer* buffer) override;
  const ObjectStatus& GetStatus() override;
  StorageError Close() override;

  // Returns a reference to the counter. While you can use this reference to later check the current value of the
  // counter, keep in mind that it has the same lifetime as the MockReader itself.
  const size_t& GetReadOperationCounter() const;

 private:
  std::shared_ptr<std::string> data_;
  std::string identifier_;
  ObjectStatus status_;
  size_t num_reads_ = 0;
};

class MockStorage : public Storage {
 public:
  std::unique_ptr<ObjectWriter> OpenForWriting(const std::string& object_identifier) override;
  std::unique_ptr<ObjectReader> OpenForReading(const std::string& object_identifier) override;
  StorageError Delete(const std::string& object_identifier) override;
  std::pair<std::vector<ObjectStatus>, StorageError> List(const std::string& object_prefix = "") override;

  // Simulates an error after `n` calls to `OpenForWriting`. The `n`th call to `OpenForWriting` will return an object
  // that will return errors for any operation.
  void SetSimulateWriteErrorAfter(int n);

 private:
  std::map<std::string, std::shared_ptr<std::string>> store_;
  std::mutex store_mutex_;
  std::atomic<bool> simulate_write_error_ = false;
  std::atomic<int> simulate_error_after_ = 0;
};

}  // namespace skyrise
