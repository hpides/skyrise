#include "abstract_storage.hpp"

#include "utils/assert.hpp"

namespace skyrise {

using Range = std::pair<size_t, size_t>;

// This is a generic implementation for storage backends that do not wish to override this function.
StorageError ObjectReader::ReadTail(size_t num_last_bytes, ByteBuffer* buffer) {
  const ObjectStatus status = GetStatus();
  if (status.GetError().IsError()) {
    return status.GetError();
  }

  const size_t first_byte = num_last_bytes < status.GetSize() ? status.GetSize() - num_last_bytes : 0;

  return Read(first_byte, status.GetSize() - 1, buffer);
}

std::vector<std::pair<size_t, size_t>> ObjectReader::BuildByteRanges(
    std::optional<std::vector<std::pair<size_t, size_t>>>& ranges_optional, const size_t object_size,
    const size_t request_size, const size_t footer_size) {
  if (ranges_optional.has_value()) {
    // Add the footer.
    ranges_optional.value().emplace_back(object_size - footer_size, object_size);
    return ranges_optional.value();
  }

  // Read every byte.
  if (object_size <= request_size) {
    return {std::make_pair(0, object_size)};
  }

  size_t number_requests = object_size / request_size;
  std::vector<std::pair<size_t, size_t>> ranges;
  ranges.reserve(number_requests + 1);

  for (size_t i = 0; i < number_requests; ++i) {
    const size_t start = i * request_size;
    ranges.emplace_back(start, start + request_size - 1);
  }

  // Add the final request.
  if ((number_requests * request_size) < object_size) {
    ranges.emplace_back(ranges.back().second + 1, object_size);
    number_requests++;
  }

  return ranges;
}

// NOLINTBEGIN(performance-unnecessary-value-param)
StorageError ObjectReader::ReadObjectAsync(const std::shared_ptr<ObjectBuffer>& /*object_buffer*/,
                                           std::optional<std::vector<std::pair<size_t, size_t>>> /*byte_ranges*/) {
  Fail("ReadObjectAsync called by an ObjectReader that does not provide an implementation.");
}
// NOLINTEND(performance-unnecessary-value-param)

}  // namespace skyrise
