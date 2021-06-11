#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "storage/backend/abstract_storage.hpp"
#include "storage/formats/abstract_format_writer.hpp"
#include "storage/table/chunk.hpp"
#include "utils/concurrent/queue.hpp"

namespace skyrise {

struct PartitionedChunkWriterConfig {
  // Required object that provides configurations to create formatters.
  std::shared_ptr<AbstractFormatWriterFactory> format_factory;

  // A thread-safe function that generates an object name, given the partition number. The numbers start with 0. This
  // function will return something like "lineitem/part00000.orc" for part=0, "lineitem/part00001.orc" for
  // part=1, etc.
  std::function<std::string(size_t part)> naming_strategy;

  // Specifies after how many rows a new object should be created. If 0, there won't be horizontal partitioning
  // within a worker (e.g., there will be one object per thread). Note that splits will only occur at chunk boundaries.
  // This means that the actual number of rows per object can be higher.
  size_t split_rows = 0;

  // Number of unprocessed chunks to be queued before `WriteChunk` starts blocking. A number greater than zero is
  // required here.
  size_t queue_capacity = 1;

  // Number of workers. This will also determine the number of objects that will be written concurrently.
  size_t num_threads = 1;
};

// PartitionedChunkWriter provides a high level interface to write chunks in a specified format to a given storage.
// Once constructed the methods `WriteChunk`, `GetError` and `HasError` can be called concurrently from multiple
// threads. Any other method is not thread-safe.
class PartitionedChunkWriter : public AbstractChunkWriter {
 public:
  explicit PartitionedChunkWriter(PartitionedChunkWriterConfig config, std::shared_ptr<Storage> storage);
  ~PartitionedChunkWriter() override;

  void Initialize(const TableColumnDefinitions& schema) override;

  // Flushes all pending write operations. There must not be any pending calls to `WriteChunk` when this function is
  // called. Also after calling this function, it is not allowed to call `WriteChunk` again since this could result in
  // the function to block forever.
  void Finalize() override;

  // Adds the given chunk to a pool of chunks that will be processed asynchronously. This function is thread-safe but
  // may not be called after `Finalize` has been called.
  void ProcessChunk(std::shared_ptr<Chunk> chunk) override;

 private:
  void StartWorkers(size_t n);
  void ProcessChunkLoop();
  void NonVirtualFinalize();

  PartitionedChunkWriterConfig config_;
  Queue<std::shared_ptr<Chunk>> queue_;
  std::vector<std::thread> threads_;
  std::shared_ptr<Storage> storage_;
  std::atomic<size_t> object_id_counter_ = 0;
  TableColumnDefinitions schema_;
};

class MemoryChunkWriter : public AbstractChunkWriter {
 public:
  void Finalize() override {}
  void Initialize(const TableColumnDefinitions& /*schema*/) override{};
  void ProcessChunk(std::shared_ptr<Chunk> chunk) override;
  const std::vector<std::shared_ptr<Chunk>>& GetChunks() { return chunks_; }

 private:
  std::mutex write_mutex_;
  std::vector<std::shared_ptr<Chunk>> chunks_;
};

using PartitionedChunkWriterFactory =
    std::function<std::shared_ptr<AbstractChunkWriter>(const std::string& name, const TableColumnDefinitions& schema)>;

}  // namespace skyrise
