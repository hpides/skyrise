#pragma once

#include "abstract_operator.hpp"
#include "compiler/physical_query_plan/operator_proxy/import_options.hpp"
#include "storage/backend/abstract_storage.hpp"
#include "storage/formats/abstract_chunk_reader.hpp"
#include "types.hpp"

namespace skyrise {

/*
 * The ImportOperator reads a set of objects containing structured table data, applies projections based on given
 * ColumnId, and returns a table.
 *
 * TODO(tobodner): Push column ids down to OrcFormatReader
 */
class ImportOperator : public AbstractOperator {
 public:
  ImportOperator(const std::vector<ObjectReference>& object_references, const std::vector<ColumnId>& column_ids,
                 const std::shared_ptr<AbstractChunkReaderFactory>& factory, const ImportFormat import_format);

  const std::string& Name() const override;

 protected:
  std::shared_ptr<const Table> OnExecute(
      const std::shared_ptr<OperatorExecutionContext>& operator_execution_context) override;
  std::shared_ptr<const TableColumnDefinitions> ExtractSchema(
      const std::shared_ptr<const skyrise::TableColumnDefinitions>& reader_schema);

  std::shared_ptr<Chunk> GetNextChunk(const std::shared_ptr<AbstractChunkReader>& reader);

 private:
  const std::vector<ObjectReference> object_references_;
  std::vector<ColumnId> column_ids_;
  const std::shared_ptr<AbstractChunkReaderFactory> factory_;
  const ImportFormat import_format_;
};
}  // namespace skyrise
