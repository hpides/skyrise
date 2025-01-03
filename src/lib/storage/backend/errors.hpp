#pragma once

#include <atomic>
#include <mutex>
#include <string>

namespace skyrise {

enum class StorageErrorType {
  kNoError = 0,
  kAlreadyExist,
  kNotFound,
  kNotReady,
  kInternalError,
  kInvalidArgument,
  kInvalidState,
  kIOError,
  kOperationNotSupported,
  kPermissionDenied,
  kTemporary,
  kUninitialized,
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

  bool IsError() const { return type_ != StorageErrorType::kNoError; }
  explicit operator bool() const { return IsError(); }

 private:
  StorageErrorType type_;
  std::string message_;
};

/**
 * A thread-safe data structure that holds a StorageError. It implements a first-error-wins strategy to handle
 * concurrent errors.
 */
class ConcurrentErrorState {
 public:
  ConcurrentErrorState() : has_error_(false), error_(StorageErrorType::kNoError) {}

  bool HasError() const { return has_error_.load(); }
  const StorageError& GetError() const;
  void SetError(const StorageError& error);

 private:
  mutable std::mutex error_mutex_;
  std::atomic_bool has_error_;
  StorageError error_;
};

}  // namespace skyrise
