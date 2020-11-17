#pragma once

#include <functional>
#include <iostream>
#include <memory>

#include "storage/types/chunk.hpp"
#include "storage/types/table_column_definition.hpp"

namespace skyrise {

// AbstractFormatter provides the interface for converting Chunks to a file format, such as CSV or ORC.
// The output is either written to a lambda function or an std::iostream object. To be able to use this
// class in a factory class, a struct type holding configurations should be made available under the name
// `Configuration`. Concrete implementations are not thread-safe.
class AbstractFormatter {
 public:
  ~AbstractFormatter() = default;

  // Initialize the formatter with a given schema. must be called exactly once before any calls to ProcessChunk() or
  // Finalize(). Initialize may already write data to the output.
  virtual void Initialize(const TableColumnDefinitions& schema) = 0;

  // ProcessChunk formats the given chunk and may write data to the output. Initialize() must be called before
  // any call to this function occurs.
  virtual void ProcessChunk(const Chunk& chunk) = 0;

  // Finalize may write pending buffers or file footers to the output. It is invalid to call Initialize() or
  // ProcessChunk() after this method is called.
  virtual void Finalize() = 0;

  void SetOutput(std::function<void(const char* data, size_t length)> callback);
  void SetOutput(std::shared_ptr<std::iostream> stream);

 protected:
  void WriteToOutput(const char* data, size_t length);

 private:
  std::function<void(const char* data, size_t length)> callback_;
  std::shared_ptr<std::iostream> stream_;
};

class AbstractFormatterFactory {
 public:
  virtual ~AbstractFormatterFactory() = default;
  virtual std::unique_ptr<AbstractFormatter> Get() = 0;
};

template <typename Formatter>
class FormatterFactory : public AbstractFormatterFactory {
 public:
  FormatterFactory(const typename Formatter::Configuration& config) : config_(config) {}

  std::unique_ptr<AbstractFormatter> Get() override { return std::make_unique<Formatter>(config_); }

 private:
  typename Formatter::Configuration config_;
};

}  // namespace skyrise
