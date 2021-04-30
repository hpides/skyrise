/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#pragma once

#include <cstddef>
#include <cstdint>
#include <iostream>
#include <string>
#include <variant>

#include "magic_enum.hpp"
#include "utils/assert.hpp"

namespace skyrise {

enum class DataType : uint8_t { kNull, kInt, kLong, kFloat, kDouble, kString };

// Represents SQL NULL value in AllTypeVariant
struct NullValue {};

using AllTypeVariant = std::variant<NullValue, int32_t, int64_t, float, double, std::string>;

/**
 * Use kNullValue instead of AllTypeVariant{}, AllTypeVariant{NullValue{}}, NullValue{}, etc. whenever a NULL value
 * needs to be represented.
 *  - Comparing any AllTypeVariant to kNullValue returns false in accordance with the ternary logic.
 *  - Use VariantIsNull() if you want to check if an AllTypeVariant represents NULL.
 */
inline const auto kNullValue = AllTypeVariant{};

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

inline bool VariantIsNull(const AllTypeVariant& variant) { return variant.index() == 0; }

// Relational operators
inline bool operator==(const NullValue&, const NullValue&) { return false; }
inline bool operator!=(const NullValue&, const NullValue&) { return false; }
inline bool operator<(const NullValue&, const NullValue&) { return false; }
inline bool operator<=(const NullValue&, const NullValue&) { return false; }
inline bool operator>(const NullValue&, const NullValue&) { return false; }
inline bool operator>=(const NullValue&, const NullValue&) { return false; }
inline NullValue operator-(const NullValue&) { return NullValue{}; }

// Hash operators
inline size_t HashValue(const NullValue&) {
  // Aggregate wants all NULLs in one bucket
  return 0;
}

// Stream operators
inline std::ostream& operator<<(std::ostream& stream, const DataType data_type) {
  return stream << magic_enum::enum_name(data_type);
}
inline std::ostream& operator<<(std::ostream& stream, const NullValue) { return stream << "NULL"; }

}  // namespace skyrise
