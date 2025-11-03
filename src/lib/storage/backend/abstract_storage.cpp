#include "abstract_storage.hpp"

namespace skyrise {

// This is a generic implementation for storage backends that do not wish to override this function.
StorageError ObjectReader::ReadTail(size_t num_last_bytes, std::vector<char>* buffer) {
  const ObjectStatus status = GetStatus();
  if (status.GetError().IsError()) {
    return status.GetError();
  }

  const size_t first_byte = num_last_bytes < status.GetSize() ? status.GetSize() - num_last_bytes : 0;

  return Read(first_byte, status.GetSize() - 1, buffer);
}

}  // namespace skyrise
