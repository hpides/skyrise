#pragma once

#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "storage/backend/abstract_storage.hpp"
#include "storage/formats/abstract_chunk_writer.hpp"
#include "storage/table/chunk.hpp"

namespace skyrise {

struct PartitionedChunkWriterConfig {
  // Required object that provides configurations to create formatters.
  std::shared_ptr<AbstractFormatWriterFactory> format_factory;

  // A function that generates an object name, given the partition number. The numbers start with 0. This function will
  // return something like "lineitem/part00000.orc" for part=0, "lineitem/part00001.orc" for part=1, etc.
  std::function<std::string(size_t part)> naming_strategy;

  // Specifies after how many rows a new object should be created. If 0, there won't be horizontal partitioning. Note
  // that splits will only occur at chunk boundaries. This means that the actual number of rows per object can be
  // higher.
  size_t split_rows = 0;
};

// PartitionedChunkWriter provides a high level interface to write chunks in a specified format to a given storage.
// NOLINTNEXTLINE(cppcoreguidelines-special-member-functions,hicpp-special-member-functions)
class PartitionedChunkWriter : public AbstractChunkWriter {
 public:
  explicit PartitionedChunkWriter(PartitionedChunkWriterConfig config, std::shared_ptr<Storage> storage);
  ~PartitionedChunkWriter() override;

  void Initialize(const TableColumnDefinitions& schema) override;

  // Flushes all pending write operations. There must not be any pending calls to WriteChunk when this function is
  // called. Also, after calling this function, it is not allowed to call WriteChunk again since this could result in
  // the function to block forever.
  void Finalize() override;

  // Adds the given chunk to a pool of chunks that will be processed asynchronously. This function may not be called
  // after Finalize has been called.
  void ProcessChunk(std::shared_ptr<const Chunk> chunk) override;

 private:
  void Flush();
  void NonVirtualFinalize();

  PartitionedChunkWriterConfig config_;
  std::shared_ptr<Storage> storage_;
  TableColumnDefinitions schema_;
  std::unique_ptr<skyrise::AbstractFormatWriter> current_formatter_;
  std::unique_ptr<ObjectWriter> current_output_object_;
  size_t num_rows_written_ = 0;
  size_t object_id_counter_ = 0;
};

class MemoryChunkWriter : public AbstractChunkWriter {
 public:
  void Finalize() override {}
  void Initialize(const TableColumnDefinitions& /*schema*/) override{};
  void ProcessChunk(std::shared_ptr<const Chunk> chunk) override;
  const std::vector<std::shared_ptr<const Chunk>>& GetChunks() { return chunks_; }

 private:
  std::vector<std::shared_ptr<const Chunk>> chunks_;
};

using PartitionedChunkWriterFactory =
    std::function<std::shared_ptr<AbstractChunkWriter>(const std::string& name, const TableColumnDefinitions& schema)>;

}  // namespace skyrise
