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
  ImportOperator(std::string bucket_name, const std::vector<std::string>& source_object_keys,
                 const std::vector<ColumnId>& column_ids, const std::shared_ptr<AbstractChunkReaderFactory>& factory);

  const std::string& Name() const override;

 protected:
  std::shared_ptr<const Table> OnExecute(
      const std::shared_ptr<OperatorExecutionContext>& operator_execution_context = nullptr) override;
  std::shared_ptr<const TableColumnDefinitions> ExtractSchema();

 private:
  const std::string bucket_name_;
  const std::vector<std::string> source_object_keys_;
  std::vector<ColumnId> column_ids_;
  const std::shared_ptr<AbstractChunkReaderFactory> factory_;
  ChunkReader reader_;
};
}  // namespace skyrise
