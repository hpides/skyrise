/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#pragma once

#include <cstddef>
#include <cstdint>
#include <iostream>
#include <string>
#include <type_traits>
#include <variant>

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
 * @return The DataType of an AllTypeVariant.
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

/**
 * Resolves a data type by calling the given @param functor with a variable of the associated type.
 */
template <typename Functor>
void ResolveDataType(DataType data_type, const Functor& functor) {
  DebugAssert(data_type != DataType::kNull, "data_type must not be null.");
  switch (data_type) {
    case DataType::kInt:
      functor(int32_t{});
      break;
    case DataType::kLong:
      functor(int64_t{});
      break;
    case DataType::kFloat:
      functor(float{});
      break;
    case DataType::kDouble:
      functor(double{});
      break;
    case DataType::kString:
      functor(std::string{});
      break;
    default:
      Fail("Unsupported DataType cannot be resolved.");
  }
}

inline bool VariantIsNull(const AllTypeVariant& variant) { return variant.index() == 0; }

inline bool IsFloatingPointDataType(const DataType data_type) {
  return data_type == DataType::kFloat || data_type == DataType::kDouble;
}

/**
 * Relational operators
 */
inline bool operator==(const NullValue& /*unused*/, const NullValue& /*unused*/) { return false; }
inline bool operator!=(const NullValue& /*unused*/, const NullValue& /*unused*/) { return false; }
inline bool operator<(const NullValue& /*unused*/, const NullValue& /*unused*/) { return false; }
inline bool operator<=(const NullValue& /*unused*/, const NullValue& /*unused*/) { return false; }
inline bool operator>(const NullValue& /*unused*/, const NullValue& /*unused*/) { return false; }
inline bool operator>=(const NullValue& /*unused*/, const NullValue& /*unused*/) { return false; }
inline NullValue operator-(const NullValue& /*unused*/) { return NullValue{}; }

/**
 * Stream operators
 */
std::ostream& operator<<(std::ostream& stream, const DataType data_type);
std::ostream& operator<<(std::ostream& stream, const NullValue null_value);
std::ostream& operator<<(std::ostream& stream, const AllTypeVariant& value);

// Hash function required by Boost.ContainerHash
// NOLINTNEXTLINE(readability-identifier-naming)
inline size_t hash_value(const NullValue& /*null_value*/) {
  // Aggregate wants all NULLs in one bucket
  return 0;
}

}  // namespace skyrise
