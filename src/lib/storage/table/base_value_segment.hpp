/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#pragma once

#include "abstract_segment.hpp"

namespace skyrise {

class BaseValueSegment : public AbstractSegment {
 public:
  using AbstractSegment::AbstractSegment;

  // Returns true if segment supports NULL values
  virtual bool IsNullable() const = 0;

  // Appends the value at the end of the segment
  virtual void Append(const AllTypeVariant& val) = 0;

  // Returns vector of NULL values.
  // Assumes that this Segment is nullable
  virtual const std::vector<bool>& NullValues() const = 0;
};

}  // namespace skyrise
