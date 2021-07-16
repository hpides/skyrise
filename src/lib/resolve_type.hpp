/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#include <functional>

#include "all_type_variant.hpp"
#include "utils/assert.hpp"

namespace skyrise {

template <typename Functor>
void ResolveType(DataType data_type, const Functor& functor) {
  switch (data_type) {
    case DataType::kNull:
      Fail("DataType must not be null.");
    case DataType::kInt:
      return functor(static_cast<int32_t>(0));
    case DataType::kLong:
      return functor(static_cast<int64_t>(0));
    case DataType::kFloat:
      return functor(static_cast<float>(0));
    case DataType::kDouble:
      return functor(static_cast<double>(0));
    case DataType::kString:
      return functor(std::string{});
    default:
      Fail("DataType is not supported.");
  }
}

}  // namespace skyrise
