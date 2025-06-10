/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#pragma once

#include <algorithm>
#include <atomic>
#include <cassert>
#include <functional>
#include <limits>
#include <memory>
#include <vector>

#include "all_type_variant.hpp"
#include "base_value_segment.hpp"
#include "storage/storage_types.hpp"
#include "types.hpp"

namespace skyrise {

// A Chunk is a horizontal partition of a table. It stores the table's data segment by segment.
// NOLINTNEXTLINE(cppcoreguidelines-special-member-functions,hicpp-special-member-functions)
class Chunk {
 public:
  Chunk(const Chunk& other) = delete;
  const Chunk& operator=(const Chunk&) = delete;

  // Constructs a new chunk from non-empty segments.
  explicit Chunk(Segments segments);

  // Atomically replaces the current segment at column_id with the passed segment
  void ReplaceSegment(size_t column_id, const std::shared_ptr<AbstractSegment>& segment);

  // Returns the number of columns, which is equal to the number of segments
  ColumnCount GetColumnCount() const;

  // Returns the number of rows
  ChunkOffset Size() const;

  // Adds a new row, given as a list of values, to the chunk.
  // All segments must be ValueSegments.
  // Note this is slow and not thread-safe and should be used for testing purposes only.
  // All underlying segments must have sufficient capacity to hold the new values.
  void Append(const std::vector<AllTypeVariant>& values);

  // Atomically accesses and returns the segment at a given position
  std::shared_ptr<AbstractSegment> GetSegment(ColumnId column_id) const;

  // Makes an estimation about the memory used by this chunk and its segments
  size_t MemoryUsageBytes() const;

 private:
  Segments segments_;
};

}  // namespace skyrise
