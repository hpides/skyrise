#include "input_handler.hpp"

#include "configuration.hpp"
#include "scheduler/worker/generic_task.hpp"
#include "storage/formats/parquet_metadata_reader.hpp"
#include "storage/formats/parquet_reader.hpp"

namespace skyrise {

std::shared_ptr<std::queue<LazyReaderConstructor>> InputHandler::CreateBufferedFormatReaders(
    const std::shared_ptr<const OperatorExecutionContext>& execution_context,
    const std::vector<ObjectReference>& object_references, const std::shared_ptr<AbstractChunkReaderFactory>& factory,
    const ImportFormat import_format, const std::optional<const std::vector<ColumnId>>& columns) {
  auto format_readers = std::make_shared<std::queue<LazyReaderConstructor>>();
  std::vector<std::shared_ptr<AbstractTask>> get_object_tasks;
  get_object_tasks.reserve(object_references.size());

  const auto object_readers = InitializeObjectReaders(execution_context, object_references);

  for (const auto& object_reference : object_references) {
    const auto size_and_reader_pair = object_readers.find(object_reference.identifier)->second;
    const size_t object_size = size_and_reader_pair.first;
    const auto object_reader = size_and_reader_pair.second;

    if (object_size < kS3ReadRequestSizeBytes) {
      // Create a task to read the object in a single request.
      get_object_tasks.push_back(std::make_shared<GenericTask>(
          [=] { ReadObjectSyncTask(object_reader, object_size, object_reference, format_readers, factory); }));
    } else {
      // Create a task to read the object async.
      get_object_tasks.push_back(std::make_shared<GenericTask>([=] {
        ReadObjectAsyncTask(object_reader, import_format, object_size, object_reference, format_readers, factory,
                            columns, object_reference.partitions);
      }));
    }
  }

  // TODO(tobodner): Add instrumentation for throughput measurements.
  // AWS_LOGSTREAM_INFO("IO_ENTERING", "");
  execution_context->GetScheduler()->ScheduleAndWaitForTasks(get_object_tasks);
  // AWS_LOGSTREAM_INFO("IO_LEAVING", "");

  return format_readers;
}

std::optional<std::vector<std::pair<size_t, size_t>>> InputHandler::PrecomputeByteRanges(
    const std::shared_ptr<ObjectReader>& object_reader, const ImportFormat import_format, const size_t object_size,
    const std::optional<const std::vector<ColumnId>>& columns, const std::optional<std::vector<int32_t>>& partitions) {
  switch (import_format) {
    case ImportFormat::kCsv:
    case ImportFormat::kOrc: {
      // TODO(tobodner): Provide an implementation for projection pushdown in ORC formatted files.
      return std::nullopt;
    }
    case ImportFormat::kParquet: {
      auto options = ParquetFormatReaderOptions();
      if (partitions.has_value()) {
        options.row_group_ids = partitions;
      }
      options.include_columns = columns;

      return ParquetFormatMetadataReader::CalculatePageOffsets(object_reader, object_size, options);
    }
    default: {
      return std::nullopt;
    }
  }
}

void InputHandler::ReadObjectAsyncTask(const std::shared_ptr<ObjectReader>& object_reader,
                                       const ImportFormat import_format, const size_t object_size,
                                       const ObjectReference& object_reference,
                                       const std::shared_ptr<std::queue<LazyReaderConstructor>>& format_readers,
                                       const std::shared_ptr<AbstractChunkReaderFactory>& factory,
                                       const std::optional<const std::vector<ColumnId>>& columns,
                                       const std::optional<std::vector<int32_t>>& partitions) {
  const auto byte_ranges = PrecomputeByteRanges(object_reader, import_format, object_size, columns, partitions);

  // TODO(tobodner): Add instrumentation for throughput measurements.
  // size_t read_size = 0;
  // for (const auto& range : byte_ranges.value()) {
  //   read_size += (range.second - range.first);
  // }
  // AWS_LOGSTREAM_INFO("OPERATOR_BYTES_CONSUMED", read_size);

  const auto object_buffer = std::make_shared<ObjectBuffer>();
  const auto result = object_reader->ReadObjectAsync(object_buffer, byte_ranges);
  Assert(!result.IsError(), "Error while loading object " + object_reference.identifier);

  std::lock_guard<std::mutex> lock_guard(queue_mutex_);
  format_readers->emplace(
      [factory, object_buffer, object_size, object_reference]() { return factory->Get(object_buffer, object_size); });
}

void InputHandler::ReadObjectSyncTask(const std::shared_ptr<ObjectReader>& object_reader, const size_t object_size,
                                      const ObjectReference& object_reference,
                                      const std::shared_ptr<std::queue<LazyReaderConstructor>>& format_readers,
                                      const std::shared_ptr<AbstractChunkReaderFactory>& factory) {
  const auto byte_buffer = std::make_shared<ByteBuffer>(object_size);
  const auto object_buffer = std::make_shared<ObjectBuffer>();
  object_buffer->AddBuffer({{0, object_size}, byte_buffer});
  const auto result = object_reader->Read(0, ObjectReader::kLastByteInFile, byte_buffer.get());
  Assert(!result.IsError(), "Error while loading object " + object_reference.identifier);

  // TODO(tobodner): Add instrumentation for throughput measurements.
  // AWS_LOGSTREAM_INFO("OPERATOR_BYTES_CONSUMED", object_size);
  std::lock_guard<std::mutex> lock_guard(queue_mutex_);
  format_readers->emplace(
      [factory, object_buffer, object_size, object_reference]() { return factory->Get(object_buffer, object_size); });
}

std::unordered_map<std::string, std::pair<size_t, const std::shared_ptr<ObjectReader>>>
InputHandler::InitializeObjectReaders(const std::shared_ptr<const OperatorExecutionContext>& execution_context,
                                      const std::vector<ObjectReference>& object_references) {
  std::vector<std::shared_ptr<AbstractTask>> get_object_size_tasks;
  get_object_size_tasks.reserve(object_references.size());
  std::unordered_map<std::string, std::pair<size_t, const std::shared_ptr<ObjectReader>>> size_and_reader_map;
  size_and_reader_map.reserve(object_references.size());

  for (const auto& object_reference : object_references) {
    get_object_size_tasks.push_back(
        std::make_shared<GenericTask>([&size_and_reader_map, &execution_context, &object_reference, this] {
          const std::shared_ptr<ObjectReader> object_reader =
              execution_context->GetStorage(object_reference.bucket_name)->OpenForReading(object_reference.identifier);

          const std::string& expected_etag = object_reference.etag;
          if (!expected_etag.empty()) {
            Assert(object_reader->GetStatus().GetChecksum() == expected_etag,
                   "The Etag does not match for object " + object_reference.identifier + ".");
          }

          Assert(!object_reader->GetStatus().GetError().IsError(), object_reader->GetStatus().GetError().GetMessage());
          const size_t object_size = object_reader->GetStatus().GetSize();

          std::lock_guard<std::mutex> lock_guard(queue_mutex_);
          size_and_reader_map.insert({object_reference.identifier, {object_size, object_reader}});
        }));
  }

  execution_context->GetScheduler()->ScheduleAndWaitForTasks(get_object_size_tasks);

  return size_and_reader_map;
}

}  // namespace skyrise
