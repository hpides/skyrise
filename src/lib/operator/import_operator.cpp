#include "import_operator.hpp"

#include "scheduler/worker/generic_task.hpp"
#include "storage/backend/abstract_storage.hpp"
#include "storage/table/table.hpp"

namespace {

const std::string kName = "Import";

}  // namespace

namespace skyrise {

ImportOperator::ImportOperator(const std::vector<ObjectReference>& object_references,
                               const std::vector<ColumnId>& column_ids,
                               const std::shared_ptr<AbstractChunkReaderFactory>& factory)
    : AbstractOperator(OperatorType::kImport),
      object_references_(object_references),
      column_ids_(column_ids),
      factory_(factory) {}

std::shared_ptr<const TableColumnDefinitions> ImportOperator::ExtractSchema(
    const std::shared_ptr<const skyrise::TableColumnDefinitions>& reader_schema) {
  Assert(reader_schema != nullptr, "The ChunkReader does not provide any table column information.");

  const size_t reader_schema_size = reader_schema->size();
  const size_t included_column_ids_size = column_ids_.size();
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

std::shared_ptr<Chunk> ImportOperator::GetNextChunk(const std::unique_ptr<AbstractChunkReader>& reader) {
  if (!reader->HasNext()) {
    return nullptr;
  }

  std::shared_ptr<Chunk> reader_chunk = reader->Next();
  if (reader_chunk == nullptr || reader_chunk->GetColumnCount() == column_ids_.size()) {
    return reader_chunk;
  } else {
    Segments segments;
    segments.reserve(column_ids_.size());

    for (const auto& column_id : column_ids_) {
      segments.push_back(reader_chunk->GetSegment(column_id));
    }
    return std::make_shared<Chunk>(segments);
  }
}

std::shared_ptr<const Table> ImportOperator::OnExecute(
    const std::shared_ptr<OperatorExecutionContext>& operator_execution_context) {
  std::shared_ptr<const skyrise::TableColumnDefinitions> loaded_table_schema;
  std::shared_ptr<const skyrise::TableColumnDefinitions> base_table_schema;

  StorageError reader_error = StorageError::Success();
  std::vector<std::shared_ptr<AbstractTask>> reader_jobs;
  reader_jobs.reserve(object_references_.size());

  std::vector<std::shared_ptr<Chunk>> chunks;
  chunks.reserve(object_references_.size());
  std::mutex chunk_store_mutex;

  // Group objects by bucket.
  std::map<std::string, std::vector<ObjectReference>> objects_per_bucket;
  for (const ObjectReference& object_reference : object_references_) {
    objects_per_bucket[object_reference.bucket_name].push_back(object_reference);
  }

  for (const auto& [bucket_name, bucket_object_references] : objects_per_bucket) {
    const std::shared_ptr<skyrise::Storage> storage = operator_execution_context->GetStorage(bucket_name);

    // Before we start the reader jobs, we load and check the table schema.
    if (base_table_schema == nullptr) {
      auto object_reader = storage->OpenForReading(bucket_object_references[0].identifier);
      base_table_schema = factory_->Get(std::move(object_reader))->GetSchema();
      loaded_table_schema = ExtractSchema(base_table_schema);
    }

    for (const auto& bucket_object_reference : bucket_object_references) {
      auto reader_job = [this, &reader_error, &base_table_schema, &chunk_store_mutex, &chunks, bucket_object_reference,
                         storage]() {
        auto object_reader = storage->OpenForReading(bucket_object_reference.identifier);

        const std::string& expected_etag = bucket_object_reference.etag;
        if (!expected_etag.empty() && object_reader->GetStatus().GetChecksum() != expected_etag) {
          reader_error = StorageError(StorageErrorType::kNotFound,
                                      "The Etag does not match for file " + bucket_object_reference.identifier + ".");
          return;
        }

        auto reader = factory_->Get(std::move(object_reader));
        const auto& reader_schema = reader->GetSchema();
        if (*base_table_schema != *reader_schema) {
          reader_error = StorageError(StorageErrorType::kInvalidState, "Schema does not match between partitions.");
          return;
        }
        std::shared_ptr<Chunk> chunk;

        while ((chunk = GetNextChunk(reader)) != nullptr && !reader_error.IsError() && !reader->HasError()) {
          const std::lock_guard<std::mutex> lock(chunk_store_mutex);
          chunks.push_back(std::move(chunk));
        }

        if (reader->HasError()) {
          reader_error = reader->GetError();
          return;
        }
      };

      reader_jobs.push_back(std::make_shared<GenericTask>(reader_job));
    }
  }

  operator_execution_context->GetScheduler()->ScheduleAndWaitForTasks(reader_jobs);

  Assert(!reader_error.IsError(), reader_error.GetMessage());

  return std::make_shared<Table>(*loaded_table_schema, std::move(chunks));
}

const std::string& ImportOperator::Name() const { return kName; }

}  // namespace skyrise
