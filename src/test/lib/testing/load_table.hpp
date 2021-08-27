#pragma once

#include <memory>

#include "storage/backend/errors.hpp"
#include "storage/table/table.hpp"

namespace skyrise {

template <typename Formatter, typename Storage>
inline std::shared_ptr<Table> LoadTable(
    const std::string& path, Storage storage,
    const typename Formatter::Configuration configuration = typename Formatter::Configuration()) {
  Formatter formatter(storage.OpenForReading(path), configuration);
  std::vector<std::shared_ptr<Chunk>> chunks;
  while (formatter.HasNext()) {
    std::shared_ptr<Chunk> next_chunk = formatter.Next();
    if (next_chunk != nullptr) {
      chunks.push_back(std::move(next_chunk));
    }
  }

  const StorageError storage_error = formatter.GetError();
  Assert(storage_error.GetType() != StorageErrorType::kNotFound, "File not found.");
  Assert(!storage_error.IsError(), storage_error.GetMessage());

  return std::make_shared<Table>(*formatter.GetSchema(), std::move(chunks));
}

}  // namespace skyrise
