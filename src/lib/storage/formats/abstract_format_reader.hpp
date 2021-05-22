#pragma once

#include "storage/backend/abstract_storage.hpp"
#include "storage/table/chunk.hpp"
#include "storage/table/table_column_definition.hpp"

namespace skyrise {

/**
 * AbstractFormatReader provides a common interface to read Chunks from data files.
 * To use this class in a factory, a struct type holding configurations should be made available under the name
 * Configuration. Concrete implementations are not thread-safe.
 */
class AbstractFormatReader {
 public:
  AbstractFormatReader();

  virtual ~AbstractFormatReader() = default;

  const std::shared_ptr<const TableColumnDefinitions>& GetSchema() const;
  virtual bool HasNext() = 0;
  virtual std::unique_ptr<Chunk> Next() = 0;

  bool HasError() const;
  const StorageError& GetError() const;

 protected:
  std::shared_ptr<const TableColumnDefinitions> schema_;
  void SetError(StorageError error);

 private:
  StorageError error_;
};

}  // namespace skyrise
