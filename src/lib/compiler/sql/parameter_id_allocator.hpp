/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#pragma once

#include <unordered_map>

#include "types.hpp"

using ValuePlaceholderID = uint16_t;

namespace skyrise {

/**
 * Allocates ParameterIDs for ValuePlaceholders and correlated expressions during SQL translation
 */
class ParameterIDAllocator {
 public:
  ParameterID Allocate();
  ParameterID AllocateForValuePlaceholder(const ValuePlaceholderID value_placeholder_id);

  const std::unordered_map<ValuePlaceholderID, ParameterID>& value_placeholders() const;

 private:
  ParameterID parameter_id_counter_{0};
  std::unordered_map<ValuePlaceholderID, ParameterID> value_placeholders_;
};

}  // namespace skyrise
