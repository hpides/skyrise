#pragma once

#include "abstract_operator.hpp"
#include "storage/backend/abstract_storage.hpp"
#include "storage/table/chunk_reader.hpp"

namespace skyrise {

/*
 * The ImportOperator reads a set of objects containing structured table data, applies projections based on given
 * ColumnId, and returns a table.
 *
 * TODO(anyone): Push column ids down to OrcFormatReader
 */
class ImportOperator : public AbstractOperator {
 public:
  ImportOperator(const std::shared_ptr<Storage>& storage, const std::vector<std::string>& objects_keys,
                 const std::vector<ColumnId>& column_ids, const std::shared_ptr<AbstractChunkReaderFactory>& factory);

  const std::string& Name() const override;

 protected:
  std::shared_ptr<const Table> OnExecute() override;
  std::shared_ptr<const TableColumnDefinitions> ExtractSchema();

 private:
  ChunkReader reader_;
  std::vector<ColumnId> column_ids_;
};
}  // namespace skyrise
