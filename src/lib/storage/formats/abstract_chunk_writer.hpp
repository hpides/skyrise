#pragma once

#include <atomic>
#include <functional>
#include <iostream>
#include <memory>
#include <mutex>

#include "storage/backend/errors.hpp"
#include "storage/table/chunk.hpp"
#include "storage/table/table_column_definition.hpp"

namespace skyrise {

/**
 * AbstractChunkWriter provides the interface for converting Chunks to a file format, such as CSV or ORC.
 * To use this class in a factory class, a struct type holding configurations should be made available under the name
 * Configuration. Concrete implementations may not be thread-safe.
 */
// NOLINTNEXTLINE(cppcoreguidelines-special-member-functions,hicpp-special-member-functions)
class AbstractChunkWriter {
 public:
  virtual ~AbstractChunkWriter() = default;
  AbstractChunkWriter() = default;
  AbstractChunkWriter(const AbstractChunkWriter&) = delete;
  void operator=(const AbstractChunkWriter&) = delete;

  /**
   * Initialize the formatter with a given schema. must be called exactly once before any calls to ProcessChunk() or
   * Finalize(). Initialize may already write data to the output.
   */
  virtual void Initialize(const TableColumnDefinitions& schema) = 0;

  /**
   * ProcessChunk formats the given chunk and may write data to the output. Initialize() must be called before
   * any call to this function occurs.
   */
  virtual void ProcessChunk(std::shared_ptr<const Chunk> chunk) = 0;

  /**
   * Finalize may write pending buffers or file footers to the output. It is invalid to call Initialize() or
   * ProcessChunk() after this method is called.
   */
  virtual void Finalize() = 0;

  bool HasError() const { return error_.IsError(); }
  const StorageError& GetError() const { return error_; }

 protected:
  void SetError(const StorageError& error) { error_ = error; }

 private:
  StorageError error_{StorageErrorType::kNoError};
};

class AbstractFormatWriter : public AbstractChunkWriter {
 public:
  void SetOutputHandler(std::function<void(const char* data, size_t length)> callback);

  size_t GetNumberOfWrittenBytes() const;

 protected:
  void WriteToOutput(const char* data, size_t length);

  size_t bytes_written_{0};

 private:
  std::function<void(const char* data, size_t length)> callback_;
};

class AbstractFormatWriterFactory {
 public:
  virtual ~AbstractFormatWriterFactory() = default;
  virtual std::unique_ptr<AbstractFormatWriter> Get() = 0;
};

template <typename Formatter>
class FormatterFactory : public AbstractFormatWriterFactory {
 public:
  explicit FormatterFactory(const typename Formatter::Configuration& config) : config_(config) {}

  std::unique_ptr<AbstractFormatWriter> Get() override { return std::make_unique<Formatter>(config_); }

 private:
  typename Formatter::Configuration config_;
};

}  // namespace skyrise
