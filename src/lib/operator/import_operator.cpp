#include "import_operator.hpp"

#include "storage/backend/abstract_storage.hpp"
#include "storage/table/chunk_reader.hpp"
#include "storage/table/table.hpp"

namespace {

const std::string kName = "Import";

}  // namespace

namespace skyrise {

ImportOperator::ImportOperator(std::string bucket_name, const std::vector<std::string>& source_object_keys,
                               const std::vector<ColumnId>& column_ids,
                               const std::shared_ptr<AbstractChunkReaderFactory>& factory)
    : AbstractOperator(OperatorType::kImport),
      bucket_name_(std::move(bucket_name)),
      source_object_keys_(source_object_keys),
      column_ids_(column_ids),
      factory_(factory) {}

std::shared_ptr<const TableColumnDefinitions> ImportOperator::ExtractSchema() {
  std::shared_ptr<const TableColumnDefinitions> reader_schema = reader_.GetSchema();

  const size_t included_column_ids_size = column_ids_.size();
  const size_t reader_schema_size = reader_schema->size();
  Assert(reader_schema != nullptr, "The ChunkReader does not provide any table column information.");
  Assert(!column_ids_.empty() && reader_schema_size >= included_column_ids_size,
         "The number of ColumnIds is empty or exceeds the amount of columns within a table.");

  if (included_column_ids_size == reader_schema_size) {
    return reader_schema;
  }

  TableColumnDefinitions schema;
  schema.reserve(included_column_ids_size);
  for (const auto& column_id : column_ids_) {
    schema.push_back((*reader_schema)[column_id]);
  }

  return std::make_shared<TableColumnDefinitions>(schema);
}

std::shared_ptr<const Table> ImportOperator::OnExecute(
    const std::shared_ptr<OperatorExecutionContext>& operator_execution_context) {
  reader_.AddObjects(factory_, operator_execution_context->GetStorage(bucket_name_), source_object_keys_);

  // TODO(anyone): Prune columns on Orc Level
  const auto schema = ExtractSchema();

  const size_t column_ids_size = column_ids_.size();
  std::vector<std::shared_ptr<Chunk>> chunks;
  while (reader_.HasNext()) {
    std::shared_ptr<Chunk> reader_chunk = reader_.Next();

    if (reader_chunk != nullptr) {
      if (reader_chunk->GetColumnCount() == column_ids_size) {
        chunks.push_back(std::move(reader_chunk));
      } else {
        Segments segments;
        segments.reserve(column_ids_size);

        for (const auto& column_id : column_ids_) {
          segments.push_back(reader_chunk->GetSegment(column_id));
        }
        chunks.push_back(std::make_shared<Chunk>(segments));
      }
    }
  }

  Assert(!reader_.HasError(), "The ChunkReader failed while reading chunks. Possible errors include " +
                                  "schema discrepancies or other types of data inconsistencies.");

  return std::make_shared<Table>(*schema, std::move(chunks));
}

const std::string& ImportOperator::Name() const { return kName; }

}  // namespace skyrise
