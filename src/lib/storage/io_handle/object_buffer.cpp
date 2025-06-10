#include "object_buffer.hpp"

#include <algorithm>

namespace skyrise {

ObjectBuffer::ObjectBuffer(const std::map<Range, std::shared_ptr<ByteBuffer>>& request_buffer)
    : request_buffer_(request_buffer) {}

void ObjectBuffer::AddBuffer(const std::pair<Range, std::shared_ptr<ByteBuffer>>& buffer) {
  std::lock_guard<std::mutex> lock(buffer_mutex_);
  request_buffer_.emplace(buffer);
}

std::shared_ptr<ByteBuffer> ObjectBuffer::MergeBuffers(
    const std::vector<std::pair<Range, std::shared_ptr<ByteBuffer>>>& buffers_to_merge, const size_t offset,
    const size_t n_bytes) {
  size_t start_offset = offset - buffers_to_merge.front().first.first;
  const size_t end_offset = buffers_to_merge.back().first.second;
  const size_t capacity = std::min(n_bytes, end_offset - offset);
  auto result_buffer = std::make_shared<ByteBuffer>(capacity);
  result_buffer->Resize(capacity);

  size_t write_position = 0;

  for (const auto& [range, buffer] : buffers_to_merge) {
    if (buffer == buffers_to_merge.back().second) {
      std::copy_n(buffer->CharData(), capacity - write_position, result_buffer->Data() + write_position);
      break;
    }
    std::copy_n(buffer->CharData() + start_offset, buffer->Size() - start_offset,
                result_buffer->Data() + write_position);
    write_position += buffer->Size() - start_offset;
    start_offset = 0;
  }

  return result_buffer;
}

// TODO(tobodner): Check if this needs to be thread safe.
std::shared_ptr<ByteBuffer> ObjectBuffer::Read(const size_t offset, const size_t n_bytes) {
  std::vector<std::pair<Range, std::shared_ptr<ByteBuffer>>> merge_buffers;
  for (const auto& [range, request_buffer] : request_buffer_) {
    if (range.first <= offset) {
      if (range.second >= offset + n_bytes) {
        // The request can be served from the current buffer.
        auto byte_buffer = std::make_shared<ByteBuffer>(request_buffer->Data() + offset - range.first, n_bytes);
        byte_buffer->Resize(n_bytes);
        return byte_buffer;
      } else if (range.second >= offset) {
        // The current buffer must be merged with M of the following buffers to serve the request for N bytes.
        merge_buffers.emplace_back(range, request_buffer);
      }
    } else if (!merge_buffers.empty() && !(range.first > offset + n_bytes)) {
      merge_buffers.emplace_back(range, request_buffer);
    }
  }

  return MergeBuffers(merge_buffers, offset, n_bytes);
}

}  // namespace skyrise
