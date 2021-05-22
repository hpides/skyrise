#pragma once

#include <functional>
#include <iostream>
#include <memory>

#include "storage/table/chunk.hpp"
#include "storage/table/table_column_definition.hpp"

namespace skyrise {
/**
 * AbstractFormatWriter provides the interface for converting Chunks to a file format, such as CSV or ORC.
 * The output is either written to a Lambda function or a std::iostream object. To use this class in a factory class, a
 * struct type holding configurations should be made available under the name Configuration. Concrete implementations
 * are not thread-safe.
 */
class AbstractFormatWriter {
 public:
  virtual ~AbstractFormatWriter() = default;

  /**
   * Initialize the formatter with a given schema. must be called exactly once before any calls to ProcessChunk() or
   * Finalize(). Initialize may already write data to the output.
   */
  virtual void Initialize(const TableColumnDefinitions& schema) = 0;

  /**
   * ProcessChunk formats the given chunk and may write data to the output. Initialize() must be called before
   * any call to this function occurs.
   */
  virtual void ProcessChunk(const Chunk& chunk) = 0;

  /**
   * Finalize may write pending buffers or file footers to the output. It is invalid to call Initialize() or
   * ProcessChunk() after this method is called.
   */
  virtual void Finalize() = 0;

  void SetOutputHandler(std::function<void(const char* data, size_t length)> callback);

 protected:
  void WriteToOutput(const char* data, size_t length);

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
  FormatterFactory(const typename Formatter::Configuration& config) : config_(config) {}

  std::unique_ptr<AbstractFormatWriter> Get() override { return std::make_unique<Formatter>(config_); }

 private:
  typename Formatter::Configuration config_;
};

}  // namespace skyrise
