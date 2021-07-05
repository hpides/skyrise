#include "chunk_reader.hpp"

namespace skyrise {

void ChunkReader::AddObjects(const std::shared_ptr<AbstractChunkReaderFactory>& factory,
                             const std::shared_ptr<Storage>& storage, const std::vector<std::string>& object_list) {
  if (object_list.empty()) {
    return;
  }

  for (const auto& object : object_list) {
    uninitialized_readers_.emplace(
        [factory, storage, object]() { return factory->Get(storage->OpenForReading(object)); });
  }

  if (schema_ == nullptr) {
    auto first_reader = InitializeNextReader();
    if (first_reader != nullptr) {
      active_reader_ = std::move(first_reader);
    }
  }
}

bool ChunkReader::HasNext() { return !HasError() && (active_reader_ != nullptr || !uninitialized_readers_.empty()); }

std::unique_ptr<Chunk> ChunkReader::Next() {
  if (active_reader_ == nullptr) {
    active_reader_ = InitializeNextReader();
    if (active_reader_ == nullptr) {
      return nullptr;
    }
  }

  auto chunk = active_reader_->Next();
  if (chunk == nullptr) {
    if (active_reader_->HasError()) {
      SetError(active_reader_->GetError());
    }
    return nullptr;
  }

  if (!active_reader_->HasNext()) {
    active_reader_ = nullptr;
  }

  return chunk;
}

std::unique_ptr<AbstractChunkReader> ChunkReader::InitializeNextReader() {
  if (uninitialized_readers_.empty()) {
    return nullptr;
  }
  auto next_initializer_callback = uninitialized_readers_.front();
  uninitialized_readers_.pop();

  std::unique_ptr<AbstractChunkReader> next_reader = next_initializer_callback();
  if (next_reader->HasError()) {
    SetError(next_reader->GetError());
    return nullptr;
  }
  if (!next_reader->HasNext()) {
    return InitializeNextReader();
  }

  auto schema = next_reader->GetSchema();

  if (schema_ == nullptr) {
    schema_ = std::move(schema);
  } else if ((*schema) != (*schema_)) {
    SetError(StorageError(StorageErrorType::kInvalidArgument, "Schema does not match."));
    return nullptr;
  }

  return next_reader;
}

}  // namespace skyrise
