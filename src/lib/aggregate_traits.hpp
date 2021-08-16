/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#pragma once

#include <cstdint>

#include "all_type_variant.hpp"
#include "types.hpp"

namespace skyrise {

/**
 * The following structs describe the different aggregate traits.
 * Given a ColumnType and an AggregateFunction, certain traits like the aggregate's data type can be deduced.
 */
template <typename ColumnType, AggregateFunction aggregate_function, class Enable = void>
struct AggregateTraits {};

// COUNT on all types.
template <typename ColumnType>
struct AggregateTraits<ColumnType, AggregateFunction::kCount> {
  using AggregateType = int64_t;
  static constexpr DataType kAggregateDataType = DataType::kLong;
};

// COUNT(DISTINCT) on all types.
template <typename ColumnType>
struct AggregateTraits<ColumnType, AggregateFunction::kCountDistinct> {
  using AggregateType = int64_t;
  static constexpr DataType kAggregateDataType = DataType::kLong;
};

// MIN/MAX/ANY on all types.
template <typename ColumnType, AggregateFunction aggregate_function>
struct AggregateTraits<ColumnType, aggregate_function,
                       typename std::enable_if_t<aggregate_function == AggregateFunction::kMin ||
                                                     aggregate_function == AggregateFunction::kMax ||
                                                     aggregate_function == AggregateFunction::kAny,
                                                 void>> {
  using AggregateType = ColumnType;
  static constexpr DataType kAggregateDataType = DataTypeFromType<ColumnType>();
};

// AVG on arithmetic types.
template <typename ColumnType, AggregateFunction aggregate_function>
struct AggregateTraits<ColumnType, aggregate_function,
                       typename std::enable_if_t<
                           aggregate_function == AggregateFunction::kAvg && std::is_arithmetic_v<ColumnType>, void>> {
  using AggregateType = double;
  static constexpr DataType kAggregateDataType = DataType::kDouble;
};

// SUM on integers.
template <typename ColumnType, AggregateFunction aggregate_function>
struct AggregateTraits<
    ColumnType, aggregate_function,
    typename std::enable_if_t<aggregate_function == AggregateFunction::kSum && std::is_integral_v<ColumnType>, void>> {
  using AggregateType = int64_t;
  static constexpr DataType kAggregateDataType = DataType::kLong;
};

// SUM on floating point numbers.
template <typename ColumnType, AggregateFunction aggregate_function>
struct AggregateTraits<
    ColumnType, aggregate_function,
    typename std::enable_if_t<aggregate_function == AggregateFunction::kSum && std::is_floating_point_v<ColumnType>,
                              void>> {
  using AggregateType = double;
  static constexpr DataType kAggregateDataType = DataType::kDouble;
};

// STDDEV_SAMP on arithmetic types.
template <typename ColumnType, AggregateFunction aggregate_function>
struct AggregateTraits<
    ColumnType, aggregate_function,
    typename std::enable_if_t<
        aggregate_function == AggregateFunction::kStandardDeviationSample && std::is_arithmetic_v<ColumnType>, void>> {
  using AggregateType = double;
  static constexpr DataType kAggregateDataType = DataType::kDouble;
};

// Invalid: AVG, SUM or STDDEV_SAMP on non-arithmetic types.
template <typename ColumnType, AggregateFunction aggregate_function>
struct AggregateTraits<
    ColumnType, aggregate_function,
    typename std::enable_if_t<!std::is_arithmetic_v<ColumnType> &&
                                  (aggregate_function == AggregateFunction::kAvg ||
                                   aggregate_function == AggregateFunction::kSum ||
                                   aggregate_function == AggregateFunction::kStandardDeviationSample),
                              void>> {
  using AggregateType = ColumnType;
  static constexpr DataType kAggregateDataType = DataType::kNull;
};

}  // namespace skyrise
