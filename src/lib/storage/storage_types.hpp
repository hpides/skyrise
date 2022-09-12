#pragma once

#include <cstdint>
#include <iostream>
#include <limits>
#include <memory>
#include <vector>

#include "types.hpp"

namespace skyrise {

inline constexpr ChunkId kInvalidChunkId{std::numeric_limits<ChunkId>::max()};
inline constexpr ChunkOffset kInvalidChunkOffset{std::numeric_limits<ChunkOffset>::max()};

struct RowId {
  bool IsNull() const { return chunk_offset == kInvalidChunkOffset; }

  bool operator<(const RowId& other) const {
    return std::tie(chunk_id, chunk_offset) < std::tie(other.chunk_id, other.chunk_offset);
  }

  bool operator==(const RowId& other) const {
    return std::tie(chunk_id, chunk_offset) == std::tie(other.chunk_id, other.chunk_offset);
  }

  friend std::ostream& operator<<(std::ostream& stream, const RowId& row_id);

  ChunkId chunk_id = kInvalidChunkId;
  ChunkOffset chunk_offset = kInvalidChunkOffset;
};

inline constexpr RowId kNullRowId = RowId{kInvalidChunkId, kInvalidChunkOffset};

inline std::ostream& operator<<(std::ostream& stream, const RowId& row_id) {
  stream << "RowId(" << row_id.chunk_id << ", " << row_id.chunk_offset << ")";
  return stream;
}

using RowIdPositionList = std::vector<RowId>;

/**
 * Chunk Default Size:
 *  This is the architecture-defined limit on the size of a single chunk. The last chunk offset is reserved for NULL
 *  as used in ReferenceSegments.
 *
 *  For a new chunk, this is the size of the pre-allocated ValueSegments. This is only relevant for chunks that
 *  contain data. Chunks that contain reference segments do not use the table's target_chunk_size at all.
 *
 *  The default chunk size was determined to give the best performance for single-threaded TPC-H, SF1. By all means,
 *  feel free to re-evaluate this. 2^16 is a good size because it means that on a unique column, dictionary
 *  requires up to 16 bits for the value ids. A chunk size of 100'000 would put us just slightly over that 16 bits,
 *  meaning that FixedSizeByteAligned vectors would use 32 instead of 16 bits. We do not use 65'536 because we need to
 *  account for NULL being encoded as a separate value id.
 */
inline constexpr ChunkOffset kChunkDefaultSize{65'535};
inline constexpr ChunkOffset kChunkMaxSize{std::numeric_limits<ChunkOffset>::max() - 1};

class AbstractSegment;
using Segments = std::vector<std::shared_ptr<AbstractSegment>>;

}  // namespace skyrise
