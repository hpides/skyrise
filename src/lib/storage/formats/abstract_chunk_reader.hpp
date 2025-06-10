#pragma once

#include "storage/backend/abstract_storage.hpp"
#include "storage/table/chunk.hpp"
#include "storage/table/table_column_definition.hpp"

namespace skyrise {

/**
 * AbstractChunkReader provides a common interface to read Chunks from data files.
 * To use this class in a factory, a struct type holding configurations should be made available under the name
 * Configuration. Concrete implementations are not thread-safe.
 */
class AbstractChunkReader {
 public:
  AbstractChunkReader();

  virtual ~AbstractChunkReader() = default;

  const std::shared_ptr<const TableColumnDefinitions>& GetSchema() const;
  virtual bool HasNext() = 0;
  virtual std::unique_ptr<Chunk> Next() = 0;

  bool HasError() const;
  const StorageError& GetError() const;

 protected:
  void SetError(StorageError error);

  std::shared_ptr<const TableColumnDefinitions> schema_;

 private:
  StorageError error_;
};

class AbstractChunkReaderFactory {
 public:
  virtual ~AbstractChunkReaderFactory() = default;
  virtual std::shared_ptr<AbstractChunkReader> Get(std::shared_ptr<ObjectBuffer> source, size_t object_size) = 0;
};

template <typename Formatter>
class FormatReaderFactory : public AbstractChunkReaderFactory {
 public:
  explicit FormatReaderFactory(
      const typename Formatter::Configuration& configuration = typename Formatter::Configuration())
      : configuration_(std::move(configuration)) {}

  std::shared_ptr<AbstractChunkReader> Get(std::shared_ptr<ObjectBuffer> source, size_t object_size) override {
    return std::make_shared<Formatter>(source, object_size, configuration_);
  }

  const typename Formatter::Configuration& Configuration() const { return configuration_; };

 private:
  typename Formatter::Configuration configuration_;
};

}  // namespace skyrise
