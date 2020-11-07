#pragma once

#include <array>
#include <ctime>
#include <functional>
#include <memory>
#include <string>

namespace skyrise {

enum class StorageErrorType {
  kNoError = 0,
  kNotFound,
  kNotReady,
  kInternalError,
  kInvalidArgument,
  kIOError,
  kOperationNotSupported,
  kPermissionDenied,
  kTemporary,
  kUnknown
};

class StorageError {
 public:
  static StorageError Success() { return StorageError(StorageErrorType::kNoError); }

  explicit StorageError(StorageErrorType type) : type_(type) {}
  StorageError(StorageErrorType type, const std::string& message) : type_(type), message_(message) {}
  StorageError(StorageErrorType type, std::string&& message) : type_(type), message_(std::move(message)) {}

  [[nodiscard]] StorageErrorType GetType() const { return type_; }
  [[nodiscard]] const std::string& GetMessage() const { return message_; }

  explicit operator bool() const { return type_ != StorageErrorType::kNoError; }

 private:
  StorageErrorType type_;
  std::string message_;
};

class ObjectStatus {
 public:
  explicit ObjectStatus(StorageError error) : error_(error) {}
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
  time_t last_modified_timestamp_;
  std::string checksum_;
  size_t size_;
  StorageError error_;
};

// ObjectWriter can be used to write data to an object. If the object does not exist, it will be created.
// Multiple calls to Write result in the data to be concatenated. This class is not thread-safe.
class ObjectWriter {
 public:
  virtual ~ObjectWriter() = default;

  // Write writes `length` bytes from the given buffer to the object. Multiple calls result in appending the data.
  virtual StorageError Write(const char* data, size_t length) = 0;
  virtual StorageError Close() = 0;
};

// ObjectReader enables read access to an object. The same instance can be used to read different parts of
// a file. This class is not thread-safe.
class ObjectReader {
 public:
  static constexpr size_t kLastByteInFile = std::numeric_limits<size_t>::max();
  virtual ~ObjectReader() = default;
  // Read reads at most `last_byte - first_byte + 1` bytes from an object (inclusive both byte indices). The callback
  // might be invoked multiple times with `length` guaranteed to be > 0. The function returns after all available bytes
  // have been read or an error occured. This function is not thread-safe.
  virtual StorageError Read(size_t first_byte, size_t last_byte,
                            std::function<void(const char* data, size_t length)> callback) = 0;
  virtual StorageError Close() = 0;
};

// Storage provides a common interface for accessing and manipulating objects. The functions are safe to call
// concurrently from within different threads.
class Storage {
 public:
  virtual ~Storage() = default;
  virtual std::unique_ptr<ObjectWriter> OpenForWriting(const std::string& object_identifier) = 0;
  virtual std::unique_ptr<ObjectReader> OpenForReading(const std::string& object_identifier) = 0;
  virtual ObjectStatus GetStatus(const std::string& object_identifier) = 0;
  virtual StorageError Delete(const std::string& object_identifier) = 0;
  virtual std::pair<std::vector<ObjectStatus>, StorageError> List(const std::string& object_prefix = "") = 0;
};

}  // namespace skyrise
