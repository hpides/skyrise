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

/**
 * @returns The DataType of an AllTypeVariant.
 *
 * Note that DataType and AllTypeVariant are defined with correlating indices.
 */
inline DataType DataTypeFromAllTypeVariant(const AllTypeVariant& all_type_variant) {
  return static_cast<DataType>(all_type_variant.index());  // NOLINT(cppcoreguidelines-pro-type-static-cast-downcast)
}

template <typename T>
constexpr DataType DataTypeFromType() {
  using RemoveConstT = typename std::remove_const<T>::type;
  if constexpr (std::is_same_v<RemoveConstT, int32_t>) {
    return DataType::kInt;
  }
  if constexpr (std::is_same_v<RemoveConstT, int64_t>) {
    return DataType::kLong;
  }
  if constexpr (std::is_same_v<RemoveConstT, float>) {
    return DataType::kFloat;
  }
  if constexpr (std::is_same_v<RemoveConstT, double>) {
    return DataType::kDouble;
  }
  if constexpr (std::is_same_v<RemoveConstT, std::string>) {
    return DataType::kString;
  }
  Fail("The given type is not a valid column type.");
}

inline bool VariantIsNull(const AllTypeVariant& variant) { return variant.index() == 0; }

inline bool IsFloatingPointDataType(const DataType data_type) {
  return data_type == DataType::kFloat || data_type == DataType::kDouble;
}

/**
 * Relational operators
 */
inline bool operator==(const NullValue&, const NullValue&) { return false; }
inline bool operator!=(const NullValue&, const NullValue&) { return false; }
inline bool operator<(const NullValue&, const NullValue&) { return false; }
inline bool operator<=(const NullValue&, const NullValue&) { return false; }
inline bool operator>(const NullValue&, const NullValue&) { return false; }
inline bool operator>=(const NullValue&, const NullValue&) { return false; }
inline NullValue operator-(const NullValue&) { return NullValue{}; }

/**
 * Stream operators
 */
inline std::ostream& operator<<(std::ostream& stream, const DataType data_type) {
  return stream << magic_enum::enum_name(data_type);
}
inline std::ostream& operator<<(std::ostream& stream, const NullValue) { return stream << "NULL"; }
inline std::ostream& operator<<(std::ostream& stream, const AllTypeVariant& value) {
  switch (DataTypeFromAllTypeVariant(value)) {
    case DataType::kString:
      stream << "'" << std::get<std::string>(value) << "'";
      break;
    case DataType::kInt:
      stream << std::get<int32_t>(value);
      break;
    case DataType::kLong:
      stream << std::get<int64_t>(value) << "L";
      break;
    case DataType::kFloat:
      stream << std::get<float>(value) << "F";
      break;
    case DataType::kDouble:
      stream << std::get<double>(value);
      break;
    default:
      Fail("Unsupported AllTypeVariant type.");
      break;
  }

  return stream;
}

// Hash function required by Boost.ContainerHash
inline size_t hash_value(const NullValue& /*null_value*/) {
  // Aggregate wants all NULLs in one bucket
  return 0;
}

}  // namespace skyrise
