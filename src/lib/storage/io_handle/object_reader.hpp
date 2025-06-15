#pragma once

#include <memory>
#include <string>
#include <vector>

#include "storage/io_handle/object_buffer.hpp"

namespace skyrise {

/**
 * Interface for reading objects from storage (e.g., S3, R2).
 */
class ObjectReader {
 public:
  virtual ~ObjectReader() = default;

  /**
   * Reads an object asynchronously.
   * @param object_buffer The buffer to store the object data
   * @param byte_ranges Optional byte ranges to read. If not provided, reads the entire object
   * @return Result of the operation
   */
  virtual Result ReadObjectAsync(const std::shared_ptr<ObjectBuffer>& object_buffer,
                                const std::optional<std::vector<std::pair<size_t, size_t>>>& byte_ranges = std::nullopt) = 0;

  /**
   * Gets the identifier of the object being read.
   * @return The object identifier
   */
  virtual std::string GetIdentifier() const = 0;
};

}  // namespace skyrise 