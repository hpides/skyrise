#include "chunk_writer.hpp"

namespace skyrise {

PartitionedChunkWriter::PartitionedChunkWriter(PartitionedChunkWriterConfig config, std::shared_ptr<Storage> storage)
    : config_(std::move(config)), storage_(std::move(storage)) {}

void PartitionedChunkWriter::Initialize(const TableColumnDefinitions& schema) { schema_ = schema; }

PartitionedChunkWriter::~PartitionedChunkWriter() { NonVirtualFinalize(); }

void PartitionedChunkWriter::Flush() {
  current_formatter_->Finalize();
  const StorageError error = current_output_object_->Close();
  if (error) {
    SetError(error);
  }
  current_formatter_ = nullptr;
  current_output_object_ = nullptr;
}

void PartitionedChunkWriter::ProcessChunk(std::shared_ptr<const Chunk> chunk) {
  if (!chunk || HasError()) {
    return;
  }

  auto writer_callback = [this](const char* data, size_t length) {
    const StorageError error = current_output_object_->Write(data, length);
    if (error) {
      this->SetError(error);
    }
  };

  if (!current_formatter_) {
    // Iff `formatter` is a `nullptr`, `output_object` is a `nullptr` too.
    current_output_object_ = storage_->OpenForWriting(config_.naming_strategy(object_id_counter_++));
    current_formatter_ = config_.format_factory->Get();
    current_formatter_->SetOutputHandler(writer_callback);
    current_formatter_->Initialize(schema_);
  }

  current_formatter_->ProcessChunk(chunk);
  if (HasError()) {
    return;
  }

  num_rows_written_ += chunk->Size();
  if (config_.split_rows != 0 && num_rows_written_ >= config_.split_rows) {
    Flush();
    num_rows_written_ = 0;
  }
}

void PartitionedChunkWriter::Finalize() { NonVirtualFinalize(); }

void PartitionedChunkWriter::NonVirtualFinalize() {
  if (current_formatter_) {
    Flush();
  }
}

void MemoryChunkWriter::ProcessChunk(std::shared_ptr<const Chunk> chunk) { chunks_.emplace_back(std::move(chunk)); }

}  // namespace skyrise
