/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#pragma once

#include <cassert>
#include <climits>
#include <mutex>
#include <optional>
#include <vector>

#include "base_value_segment.hpp"
#include "storage/storage_types.hpp"
#include "storage/table/chunk.hpp"
#include "utils/string.hpp"

namespace skyrise {

// ValueSegment is a specific segment type that stores all its values in a vector.
template <typename T>
class ValueSegment : public BaseValueSegment {
 public:
  explicit ValueSegment(bool nullable = false, ChunkOffset capacity = kChunkDefaultSize);

  // Creates a ValueSegment with the given values.
  explicit ValueSegment(std::vector<T>&& values);

  // Creates a ValueSegment with the given values.
  // The length of values and null_values must be equal.
  explicit ValueSegment(std::vector<T>&& values, std::vector<bool>&& null_values);

  // Returns the value at a certain position. If you want to write efficient operators, back off!
  // Use values() and null_values() to get the vectors and check the content yourself.
  // chunk_offset must be a valid offset within this chunk.
  AllTypeVariant operator[](ChunkOffset chunk_offset) const override;

  // Returns whether a value is NULL
  bool IsNull(ChunkOffset chunk_offset) const;

  // Returns the value at a certain position.
  // Only use if you are certain that no NULL values are present.
  // chunk_offset must be a valid offset within this chunk.
  T Get(ChunkOffset chunk_offset) const;

  // Returns the value at a certain position.
  std::optional<T> GetTypedValue(ChunkOffset chunk_offset) const {
    // performance critical - not in cpp to help with inlining
    // Column supports NULL values and value is NULL
    if (IsNullable() && (*null_values_)[chunk_offset]) {
      return std::nullopt;
    }
    return values_[chunk_offset];
  }

  // Adds a value to the end of the segment. Not thread-safe. May fail if ValueSegment was not initially created with
  // sufficient capacity.
  // val may only contain NULL values, if IsNullable() is true.
  void Append(const AllTypeVariant& val) final;

  // Returns all values. This is the preferred method to check a value at a certain index. Usually you need to access
  // more than a single value anyway, e.g., auto& values = segment.values(); and then: values.at(i); in your loop.
  const std::vector<T>& Values() const;
  std::vector<T>& Values();

  // Returns whether segment supports NULL values.
  bool IsNullable() const final;

  // Returns a NULL value vector that indicates whether a value is NULL with true at position i.
  // Call this only, if IsNullable() is true.
  // This is the preferred method to check a for a NULL value at a certain index.
  // Usually you need to access more than a single value anyway.
  const std::vector<bool>& NullValues() const final;

  // Writing a vector<bool> is not thread-safe. By only exposing the vector as a const reference, we force people to go
  // through this thread-safe method. By design, this does not take a bool argument. All entries are false (i.e., not
  // NULL) by default. Setting them to false again is unnecessarily expensive and changing them from true to false
  // should never be necessary.
  void SetNullValue(ChunkOffset chunk_offset);

  // Return the number of entries in the segment.
  ChunkOffset Size() const final;

  // Resizes the ValueSegment.
  // ValueSegments should not be shrunk or resized beyond their original capacity
  void Resize(size_t size);

  size_t MemoryUsage() const override;

 protected:
  std::vector<T> values_;
  std::optional<std::vector<bool>> null_values_;

  // Protects set_null_value. Does not need to be acquired for reads, as we expect modifications to vector<bool> to be
  // atomic.
  std::mutex null_value_modification_mutex_;
};

}  // namespace skyrise
