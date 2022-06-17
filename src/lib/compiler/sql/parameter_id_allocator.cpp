/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#include "parameter_id_allocator.hpp"

#include "utils/assert.hpp"

namespace skyrise {

ParameterID ParameterIDAllocator::Allocate() { return static_cast<ParameterID>(parameter_id_counter_++); }

ParameterID ParameterIDAllocator::AllocateForValuePlaceholder(const ValuePlaceholderID value_placeholder_id) {
  const auto parameter_id = Allocate();
  const auto is_unique = value_placeholders_.emplace(value_placeholder_id, parameter_id).second;
  Assert(is_unique, "Duplicate ValuePlaceholderID");

  return parameter_id;
}

const std::unordered_map<ValuePlaceholderID, ParameterID>& ParameterIDAllocator::value_placeholders() const {
  return value_placeholders_;
}

}  // namespace skyrise
