#pragma once

#include <map>
#include <memory>
#include <mutex>
#include <vector>

#include "byte_buffer.hpp"

namespace skyrise {

using Range = std::pair<size_t, size_t>;

/**
 * An object buffer manages a number of buffers that represent an object in memory. In addition, the object buffer
 * manages the indirection of the original byte range to the associated buffer, so that the object buffer can be
 * accessed in the same way as the regular object.
 */
// NOLINTNEXTLINE(cppcoreguidelines-pro-type-member-init,hicpp-member-init)
class ObjectBuffer {
 public:
  ObjectBuffer() = default;
  explicit ObjectBuffer(const std::map<Range, std::shared_ptr<ByteBuffer>>& request_buffer);

  /**
   * The Read function is the main interaction to access the data of an object buffer.
   * It takes the offset and the number of bytes to be read and returns a buffer with the data.
   */
  std::shared_ptr<ByteBuffer> Read(const size_t offset, const size_t n_bytes);

  /**
   * This function adds another buffer that is managed by the object buffer.
   */
  void AddBuffer(const std::pair<Range, std::shared_ptr<ByteBuffer>>& buffer);

 private:
  static std::shared_ptr<ByteBuffer> MergeBuffers(
      const std::vector<std::pair<Range, std::shared_ptr<ByteBuffer>>>& buffers_to_merge, const size_t offset,
      const size_t n_bytes);

  // The map holds the buffers ordered by Range.
  std::map<Range, std::shared_ptr<ByteBuffer>> request_buffer_;
  std::mutex buffer_mutex_;
};

}  // namespace skyrise
