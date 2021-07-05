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

}  // namespace skyrise
