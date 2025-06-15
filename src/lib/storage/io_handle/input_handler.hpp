#pragma once

#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <unordered_map>
#include <vector>

#include "compiler/physical_query_plan/operator_proxy/import_options.hpp"
#include "operator/execution_context.hpp"
#include "scheduler/worker/abstract_task.hpp"
#include "storage/backend/abstract_storage.hpp"
#include "storage/io_handle/object_buffer.hpp"
#include "types.hpp"

namespace skyrise {

class AbstractChunkReaderFactory;

using LazyReaderConstructor = std::function<std::shared_ptr<AbstractChunkReader>()>;

/**
 * The input handler receives a number of objects to be loaded and optionally the required columns. For each object, the
 * input handler creates a chunk reader factory and provides the in-memory resident data for the chunk reader. This
 * class utilizes multiple threads at various points to hide I/O latency and optimize access to remote cloud storage.
 */
class InputHandler {
 public:
  std::shared_ptr<std::queue<LazyReaderConstructor>> CreateBufferedFormatReaders(
      const std::shared_ptr<const OperatorExecutionContext>& execution_context,
      const std::vector<ObjectReference>& object_references, const std::shared_ptr<AbstractChunkReaderFactory>& factory,
      const ImportFormat import_format, const std::optional<const std::vector<ColumnId>>& columns);

  std::optional<std::vector<std::pair<size_t, size_t>>> PrecomputeByteRanges(
      const std::shared_ptr<ObjectReader>& object_reader, const ImportFormat import_format, const size_t object_size,
      const std::optional<const std::vector<ColumnId>>& columns, const std::optional<std::vector<int32_t>>& partitions);

  void ReadObjectAsyncTask(const std::shared_ptr<ObjectReader>& object_reader,
                           const ImportFormat import_format, const size_t object_size,
                           const ObjectReference& object_reference,
                           const std::shared_ptr<std::queue<LazyReaderConstructor>>& format_readers,
                           const std::shared_ptr<AbstractChunkReaderFactory>& factory,
                           const std::optional<const std::vector<ColumnId>>& columns,
                           const std::optional<std::vector<int32_t>>& partitions,
                           const std::shared_ptr<AbstractTask>& task);

  void ReadObjectSyncTask(const std::shared_ptr<ObjectReader>& object_reader, const size_t object_size,
                          const ObjectReference& object_reference,
                          const std::shared_ptr<std::queue<LazyReaderConstructor>>& format_readers,
                          const std::shared_ptr<AbstractChunkReaderFactory>& factory);

 private:
  /*
   * Fetches the object sizes concurrently, i.e., one concurrent head request per object.
   * Returns a map with the size of each object and the object reader which was used to retrieve it's size, so that the
   * object reader instance can be reused.
   */
  std::unordered_map<std::string, std::pair<size_t, const std::shared_ptr<ObjectReader>>> InitializeObjectReaders(
      const std::shared_ptr<const OperatorExecutionContext>& execution_context,
      const std::vector<ObjectReference>& object_references);

  std::mutex queue_mutex_;
};

}  // namespace skyrise
