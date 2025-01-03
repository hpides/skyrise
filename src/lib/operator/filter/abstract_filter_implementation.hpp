/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#pragma once

#include "storage/storage_types.hpp"

namespace skyrise {

/**
 * The base class of all filter implementations.
 */
class AbstractFilterImplementation {
 public:
  virtual ~AbstractFilterImplementation() = default;

  virtual std::string Description() const = 0;

  virtual std::shared_ptr<RowIdPositionList> FilterChunk(ChunkId chunk_id) = 0;
};

}  // namespace skyrise
