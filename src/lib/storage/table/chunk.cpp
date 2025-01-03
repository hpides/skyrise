/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#include "chunk.hpp"

#include "utils/assert.hpp"

namespace skyrise {

Chunk::Chunk(Segments segments) : segments_(std::move(segments)) {}

void Chunk::ReplaceSegment(size_t column_id, const std::shared_ptr<AbstractSegment>& segment) {
  std::atomic_store(&segments_.at(column_id), segment);
}

void Chunk::Append(const std::vector<AllTypeVariant>& values) {
  auto segment_it = segments_.cbegin();
  auto value_it = values.cbegin();
  for (; segment_it != segments_.cend(); ++segment_it, ++value_it) {
    const auto& base_value_segment = std::dynamic_pointer_cast<BaseValueSegment>(*segment_it);
    base_value_segment->Append(*value_it);
  }
}

std::shared_ptr<AbstractSegment> Chunk::GetSegment(ColumnId column_id) const {
  return std::atomic_load(&segments_.at(column_id));
}

ColumnCount Chunk::GetColumnCount() const { return segments_.size(); }

ChunkOffset Chunk::Size() const { return segments_.empty() ? 0 : GetSegment(0)->Size(); }

size_t Chunk::MemoryUsageBytes() const {
  auto bytes = size_t{sizeof(*this)};

  for (const auto& segment : segments_) {
    bytes += segment->MemoryUsage();
  }

  return bytes;
}

}  // namespace skyrise
