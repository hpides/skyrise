/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#pragma once

#include <valarray>

#include "expression/aggregate_expression.hpp"
#include "operator/abstract_operator.hpp"
#include "types.hpp"

namespace skyrise {

/**
 * The AggregateFunctionBuilder is used to create the lambda function that will be used by
 * the AggregateVisitor. It is a separate class because methods cannot be partially specialized.
 * Therefore, we partially specialize the whole class and define the GetAggregateFunction anew every time.
 */
template <typename ColumnDataType, typename AggregateType, AggregateFunction AggregateFunction>
class AggregateFunctionBuilder {
 public:
  void GetAggregateFunction() { Fail("Invalid aggregate function."); }
};

using StandardDeviationSampleData = std::array<double, 4>;
using StringAggAccumulator = std::vector<std::pair<RowId, std::string>>;

template <typename ColumnDataType, typename AggregateType>
class AggregateFunctionBuilder<ColumnDataType, AggregateType, AggregateFunction::kMin> {
 public:
  auto GetAggregateFunction() {
    return [](const ColumnDataType& new_value, const size_t aggregate_count, AggregateType& accumulator,
              const RowId& /*row_id*/) {
      // We need to check if we have already seen a value before (aggregate_count > 0). Otherwise, the accumulator
      // holds an invalid value. While we might initialize the accumulator with the smallest possible numerical value,
      // this approach does not work for MAX on strings. To keep the code simple, we check aggregate_count here.
      if (aggregate_count == 0 || new_value < accumulator) {
        // New minimum found.
        accumulator = new_value;
      }
    };
  }
};

template <typename ColumnDataType, typename AggregateType>
class AggregateFunctionBuilder<ColumnDataType, AggregateType, AggregateFunction::kMax> {
 public:
  auto GetAggregateFunction() {
    return [](const ColumnDataType& new_value, const size_t aggregate_count, AggregateType& accumulator,
              const RowId& /*row_id*/) {
      if (aggregate_count == 0 || new_value > accumulator) {
        // New maximum was found.
        accumulator = new_value;
      }
    };
  }
};

template <typename ColumnDataType, typename AggregateType>
class AggregateFunctionBuilder<ColumnDataType, AggregateType, AggregateFunction::kStringAgg> {
 public:
  auto GetAggregateFunction() {
    return [](const ColumnDataType& new_value, const size_t /*aggregate_count*/, StringAggAccumulator& accumulator,
              const RowId& row_id) {
      if constexpr (std::is_same_v<ColumnDataType, std::string>) {
        accumulator.push_back({row_id, static_cast<ColumnDataType>(new_value)});
      } else {
        Fail("STRING_AGG requires string column data type.");
      }
    };
  }
};

template <typename ColumnDataType, typename AggregateType>
class AggregateFunctionBuilder<ColumnDataType, AggregateType, AggregateFunction::kSum> {
 public:
  auto GetAggregateFunction() {
    return [](const ColumnDataType& new_value, const size_t /*aggregate_count*/, AggregateType& accumulator,
              const RowId& /*row_id*/) {
      // Add the new value to the SUM. No need to check if this is the first value, as SUM is only defined on numerical
      // values and the accumulator is initialized with 0.
      accumulator += static_cast<AggregateType>(new_value);
    };
  }
};

template <typename ColumnDataType, typename AggregateType>
class AggregateFunctionBuilder<ColumnDataType, AggregateType, AggregateFunction::kAvg> {
 public:
  auto GetAggregateFunction() {
    // We reuse SUM here, as updating an average value for every row is costly and prone to problems regarding
    // precision. To get the average, the aggregate operator needs to count the number of elements contributing to this
    // SUM, and divide the final SUM by that number.
    return AggregateFunctionBuilder<ColumnDataType, AggregateType, AggregateFunction::kSum>().GetAggregateFunction();
  }
};

template <typename ColumnDataType, typename AggregateType>
class AggregateFunctionBuilder<ColumnDataType, AggregateType, AggregateFunction::kStandardDeviationSample> {
 public:
  auto GetAggregateFunction() {
    return [](const ColumnDataType& new_value, const size_t /*aggregate_count*/,
              StandardDeviationSampleData& accumulator, const RowId& /*row_id*/) {
      if constexpr (std::is_arithmetic_v<ColumnDataType>) {
        // Welford's online algorithm (https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Welford).
        // For a new value, compute the new count, new mean and the new squared_distance_from_mean.

        // Get the values.
        double& count = accumulator[0];  // We could probably reuse aggregate_count here
        double& mean = accumulator[1];
        double& squared_distance_from_mean = accumulator[2];
        double& result = accumulator[3];

        // Update the values.
        ++count;
        const double delta = static_cast<double>(new_value) - mean;
        mean += delta / count;
        squared_distance_from_mean += delta * (static_cast<double>(new_value) - mean);

        if (count > 1) {
          // The SQL standard defines VAR_SAMP (which is the basis of STDDEV_SAMP) as NULL if the number of values is 1.
          const double variance = squared_distance_from_mean / (count - 1);
          result = std::sqrt(variance);
        }
      } else {
        Fail("StandardDeviationSample not available for non-arithmetic types.");
      }
    };
  }
};

template <typename ColumnDataType, typename AggregateType>
class AggregateFunctionBuilder<ColumnDataType, AggregateType, AggregateFunction::kCount> {
 public:
  auto GetAggregateFunction() {
    return [](const ColumnDataType&, const size_t /*aggregate_count*/, AggregateType& /*accumulator*/,
              const RowId& /*row_id*/) {};
  }
};

template <typename ColumnDataType, typename AggregateType>
class AggregateFunctionBuilder<ColumnDataType, AggregateType, AggregateFunction::kCountDistinct> {
 public:
  auto GetAggregateFunction() {
    return [](const ColumnDataType&, const size_t /*aggregate_count*/, AggregateType& /*accumulator*/,
              const RowId& /*row_id*/) {};
  }
};

/**
 * Operator to aggregate columns by certain functions, such as MIN, MAX, SUM, AVERAGE, COUNT and STDDEV_SAMP. The output
 * is a table with value segments. As with most operators we do not guarantee a stable operation with regards to
 * positions - i.e. your sorting order.
 */
class AbstractAggregateOperator : public AbstractOperator {
 public:
  AbstractAggregateOperator(std::shared_ptr<AbstractOperator> input_operator,
                            std::vector<std::shared_ptr<AggregateExpression>> aggregates,
                            std::vector<ColumnId> group_by_column_ids);

  const std::vector<std::shared_ptr<AggregateExpression>>& Aggregates() const;

  const std::vector<ColumnId>& GroupByColumnIds() const;

  const std::string& Name() const override = 0;

  std::string Description(DescriptionMode description_mode) const override;

 protected:
  void ValidateAggregates() const;

  Segments output_segments_;
  const std::vector<std::shared_ptr<AggregateExpression>> aggregates_;
  const std::vector<ColumnId> group_by_column_ids_;

  TableColumnDefinitions output_column_definitions_;
};

}  // namespace skyrise
