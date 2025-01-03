/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#pragma once

#include "all_type_variant.hpp"
#include "storage/storage_types.hpp"
#include "storage/table/chunk.hpp"

namespace skyrise {

// AbstractSegment is the base class for all segment types,
// e.g., ValueSegment, ReferenceSegment (to be implemented later)
// NOLINTNEXTLINE(cppcoreguidelines-special-member-functions,hicpp-special-member-functions)
class AbstractSegment {
 public:
  explicit AbstractSegment(const DataType data_type) : data_type_(data_type) {}
  AbstractSegment(const AbstractSegment&) = delete;
  AbstractSegment& operator=(const AbstractSegment&) = delete;

  virtual ~AbstractSegment() = default;

  DataType GetDataType() const { return data_type_; }

  virtual AllTypeVariant operator[](ChunkOffset chunk_offset) const = 0;

  virtual ChunkOffset Size() const = 0;

  // Estimates how much memory the segment is using.
  // Might be inaccurate, especially if the segment contains non-primitive data,
  // such as strings who memory usage is implementation defined
  virtual size_t MemoryUsage() const = 0;

 private:
  const DataType data_type_;
};

}  // namespace skyrise
