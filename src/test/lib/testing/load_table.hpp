#pragma once

#include <memory>

#include "storage/backend/errors.hpp"
#include "storage/formats/abstract_chunk_reader.hpp"
#include "storage/io_handle/mock_object_buffer.hpp"
#include "storage/table/table.hpp"

namespace skyrise {

/*
 * Utility function that takes a storage backend, the path and an optional
 * configuration. Returns a format reader to read the object from the in-memory buffer.
 *
 * Note that this function's purpose is only for testing. For real use cases, use the InputHandler.
 */
template <typename FormatReader>
static std::shared_ptr<FormatReader> BuildFormatReader(
    const std::shared_ptr<Storage>& storage_backend, const std::string& path,
    const typename FormatReader::Configuration configuration = typename FormatReader::Configuration()) {
  const auto factory = std::make_shared<FormatReaderFactory<FormatReader>>(configuration);
  auto mock_object_buffer = std::make_shared<MockObjectBuffer>(path, storage_backend);

  return std::static_pointer_cast<FormatReader>(factory->Get(mock_object_buffer, mock_object_buffer->BufferSize()));
}

template <typename Formatter, typename StorageBackend>
inline std::shared_ptr<Table> LoadTable(
    const std::string& path, std::shared_ptr<StorageBackend> storage,
    const typename Formatter::Configuration configuration = typename Formatter::Configuration()) {
  auto reader = BuildFormatReader<Formatter>(storage, path, configuration);

  const StorageError storage_error = reader->GetError();
  Assert(storage_error.GetType() != StorageErrorType::kNotFound, "File not found.");
  Assert(!storage_error.IsError(), storage_error.GetMessage());

  std::vector<std::shared_ptr<Chunk>> chunks;
  while (reader->HasNext()) {
    auto next_chunk = reader->Next();
    chunks.push_back(std::move(next_chunk));
  }

  return std::make_shared<Table>(*reader->GetSchema(), std::move(chunks));
}

}  // namespace skyrise
