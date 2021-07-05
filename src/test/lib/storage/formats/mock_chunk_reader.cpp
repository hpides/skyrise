#include "mock_chunk_reader.hpp"

namespace skyrise {

bool MockChunkReader::HasNext() { return !HasError() && available_chunks_ > 0; }

std::unique_ptr<Chunk> skyrise::MockChunkReader::Next() {
  available_chunks_--;

  if (this->HasError()) {
    return nullptr;
  }

  if (config_.error && (config_.num_chunks - available_chunks_) == config_.num_chunks_until_error) {
    SetError(StorageError(StorageErrorType::kUnknown, "Error during chunk processing."));
    return nullptr;
  }
  return std::make_unique<Chunk>(segments_);
}

MockChunkReader::MockChunkReader(std::unique_ptr<ObjectReader> /*source*/, MockChunkReader::Configuration configuration)
    : available_chunks_(configuration.num_chunks), config_(std::move(configuration)) {
  Assert(config_.num_chunks_until_error <= config_.num_chunks,
         "Number of chunks until error cannot be greater than the number of total chunks.");

  if (config_.error && config_.num_chunks_until_error == 0) {
    SetError(StorageError(StorageErrorType::kUnknown, "Error during initialization."));
    return;
  }

  GenerateSegments();
  schema_ = config_.schema;
}

void MockChunkReader::GenerateSegments() {
  for (const auto& generator : config_.generators) {
    segments_.emplace_back(generator.Generate());
  }
}

}  // namespace skyrise
