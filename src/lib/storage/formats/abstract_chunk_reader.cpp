#include "abstract_chunk_reader.hpp"

namespace skyrise {

AbstractChunkReader::AbstractChunkReader() : error_(StorageErrorType::kNoError) {}

void AbstractChunkReader::SetError(StorageError error) { error_ = std::move(error); }

bool AbstractChunkReader::HasError() const { return error_.IsError(); }

const StorageError& AbstractChunkReader::GetError() const { return error_; }

const std::shared_ptr<const TableColumnDefinitions>& AbstractChunkReader::GetSchema() const { return schema_; }

}  // namespace skyrise
