#pragma once

#include <array>
#include <ctime>
#include <functional>
#include <memory>
#include <string>

#include "errors.hpp"
#include "storage/io_handle/byte_buffer.hpp"
#include "storage/io_handle/object_buffer.hpp"

namespace skyrise {

class ObjectStatus {
 public:
  ObjectStatus() : error_(StorageErrorType::kUninitialized) {}
  explicit ObjectStatus(StorageError error) : error_(std::move(error)) {}
  ObjectStatus(std::string identifier, time_t last_modified, std::string checksum, size_t object_size)
      : identifier_(std::move(identifier)),
        last_modified_timestamp_(last_modified),
        checksum_(std::move(checksum)),
        size_(object_size),
        error_(StorageErrorType::kNoError) {}

  const std::string& GetIdentifier() const { return identifier_; }
  const std::string& GetChecksum() const { return checksum_; }
  time_t GetLastModifiedTimestamp() const { return last_modified_timestamp_; }
  size_t GetSize() const { return size_; }
  const StorageError& GetError() const { return error_; }

 private:
  std::string identifier_;
  time_t last_modified_timestamp_{0};
  std::string checksum_;
  size_t size_{0};
  StorageError error_;
};

/**
 * ObjectWriter can be used to write data to an object. If the object does not exist, it will be created.
 * Multiple calls to Write result in the data to be concatenated. This class is not thread-safe.
 */
class ObjectWriter {
 public:
  virtual ~ObjectWriter() = default;

  /**
   * Write writes `length` bytes from the given buffer to the object. Multiple calls result in appending the data.
   */
  virtual StorageError Write(const char* data, size_t length) = 0;
  virtual StorageError Close() = 0;
};

/**
 * ObjectReader enables read access to an object. The same instance can be used to read different parts of
 * a file. This class is not thread-safe.
 */
class ObjectReader {
 public:
  static constexpr size_t kLastByteInFile = std::numeric_limits<size_t>::max();
  virtual ~ObjectReader() = default;
  /**
   * Read reads at most `last_byte - first_byte + 1` bytes from an object (inclusive both byte indices). The data is
   * written to buffer, which is cleared before writing the first byte. For performance reasons the caller should
   * allocate enough memory to hold the response (e.g. using reserve). This function is not thread-safe.
   */
  virtual StorageError Read(size_t first_byte, size_t last_byte, ByteBuffer* buffer) = 0;

  /**
   * Asynchronously reads an object into the object buffer.
   * If no byte ranges are passed to the function, it will read every byte of the object into the buffer.
   */
  virtual StorageError ReadObjectAsync(const std::shared_ptr<ObjectBuffer>& object_buffer,
                                       std::optional<std::vector<std::pair<size_t, size_t>>> byte_ranges);

  /**
   * Reads `num_last_bytes` from the end of the object. If the object is smaller than the requested number of bytes,
   * this function reads the whole object. Some storage backends provide optimizations that do not need to receive the
   * size of an objects for this call.
   */
  virtual StorageError ReadTail(size_t num_last_bytes, ByteBuffer* buffer);
  virtual const ObjectStatus& GetStatus() = 0;
  virtual StorageError Close() = 0;

  static std::vector<std::pair<size_t, size_t>> BuildByteRanges(
      std::optional<std::vector<std::pair<size_t, size_t>>>& ranges_optional, const size_t object_size,
      const size_t request_size, const size_t footer_size);

 protected:
  ObjectStatus status_;
};

/**
 * Storage provides a common interface for accessing and manipulating objects. The functions are safe to call
 * concurrently from within different threads.
 */
class Storage {
 public:
  virtual ~Storage() = default;
  virtual std::unique_ptr<ObjectWriter> OpenForWriting(const std::string& object_identifier) = 0;
  virtual std::unique_ptr<ObjectReader> OpenForReading(const std::string& object_identifier) = 0;
  ObjectStatus GetStatus(const std::string& object_identifier) {
    return OpenForReading(object_identifier)->GetStatus();
  };
  virtual StorageError Delete(const std::string& object_identifier) = 0;
  virtual std::pair<std::vector<ObjectStatus>, StorageError> List(const std::string& object_prefix = "") = 0;
};

}  // namespace skyrise
