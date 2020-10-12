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

#include "all_variants.hpp"
#include "base_value_segment.hpp"

namespace skyrise {

using Segments = std::vector<std::shared_ptr<AbstractSegment>>;

// A Chunk is a horizontal partition of a table. It stores the table's data segment by segment.
class Chunk {
 public:
  // This is the architecture-defined limit on the size of a single chunk. The last chunk offset is reserved for NULL
  // as used in ReferenceSegments.
  static constexpr ChunkOffset kMaxSize = std::numeric_limits<ChunkOffset>::max() - 1;

  // For a new chunk, this is the size of the pre-allocated ValueSegments. This is only relevant for chunks that
  // contain data. Chunks that contain reference segments do not use the table's target_chunk_size at all.
  //
  // The default chunk size was determined to give the best performance for single-threaded TPC-H, SF1. By all means,
  // feel free to re-evaluate this. 2^16 is a good size because it means that on a unique column, dictionary
  // requires up to 16 bits for the value ids. A chunk size of 100'000 would put us just slightly over that 16 bits,
  // meaning that FixedSizeByteAligned vectors would use 32 instead of 16 bits. We do not use 65'536 because we need to
  // account for NULL being encoded as a separate value id.
  static constexpr ChunkOffset kDefaultSize = 65'535;

  Chunk(const Chunk& other) = delete;
  const Chunk& operator=(const Chunk&) = delete;

  // Constructs a new chunk from non-empty segments.
  Chunk(Segments segments);

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
  std::shared_ptr<AbstractSegment> GetSegment(ColumnID column_id) const;

  // Makes an estimation about the memory used by this chunk and its segments
  size_t MemoryUsage() const;

 private:
  Segments segments_;
};

}  // namespace skyrise
