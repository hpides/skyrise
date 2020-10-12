/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#pragma once

#include <cstddef>
#include <cstdint>
#include <iostream>
#include <limits>
#include <string>
#include <variant>

#include "magic_enum.hpp"
#include "null_value.hpp"
#include "utils/assert.hpp"

namespace skyrise {

enum class DataType : uint8_t { kNull, kInt, kLong, kFloat, kDouble, kString };
using AllTypeVariant = std::variant<NullValue, int32_t, int64_t, float, double, std::string>;
inline bool VariantIsNull(const AllTypeVariant& variant) { return (variant.index() == 0); }

// Use this instead of AllTypeVariant{}, AllTypeVariant{NullValue{}}, NullValue{}, etc.
// whenever a null value needs to be represented
// Comparing any AllTypeVariant to NULL_VALUE returns false in accordance with the ternary logic
// Use variant_is_null() if you want to check if an AllTypeVariant is null
static const auto kNullValue = AllTypeVariant{};

using ChunkOffset = std::size_t;
using ColumnCount = std::size_t;
using ColumnID = std::size_t;
constexpr ChunkOffset kInvalidChunkOffset{std::numeric_limits<ChunkOffset>::max()};

inline std::ostream& operator<<(std::ostream& stream, const DataType data_type) {
  return stream << magic_enum::enum_name(data_type);
}

template <typename T>
inline constexpr DataType DataTypeFromType() {
  if constexpr (std::is_same_v<T, int32_t>) {
    return DataType::kInt;
  }
  if constexpr (std::is_same_v<T, int64_t>) {
    return DataType::kLong;
  }
  if constexpr (std::is_same_v<T, float>) {
    return DataType::kFloat;
  }
  if constexpr (std::is_same_v<T, double>) {
    return DataType::kDouble;
  }
  if constexpr (std::is_same_v<T, std::string>) {
    return DataType::kString;
  }

  static_assert(std::is_same_v<T, int32_t> || std::is_same_v<T, int64_t> || std::is_same_v<T, float> ||
                    std::is_same_v<T, double> || std::is_same_v<T, std::string>,
                "Type not a valid column type.");
}

}  // namespace skyrise
