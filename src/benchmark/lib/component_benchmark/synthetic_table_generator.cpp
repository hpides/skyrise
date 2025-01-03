#include "synthetic_table_generator.hpp"

#include <random>
#include <string>
#include <vector>

#include <boost/math/distributions/pareto.hpp>
#include <boost/math/distributions/skew_normal.hpp>
#include <boost/math/distributions/uniform.hpp>

#include "all_type_variant.hpp"
#include "storage/table/chunk.hpp"
#include "storage/table/table.hpp"
#include "storage/table/value_segment.hpp"
#include "types.hpp"
#include "utils/random.hpp"

namespace skyrise {

namespace {

using TableOffset = uint64_t;

template <typename T>
std::vector<T> GenerateValues(const std::vector<int>& values) {
  std::vector<T> result;
  result.reserve(values.size());

  for (const auto& value : values) {
    result.push_back(SyntheticTableGenerator::GenerateValue<T>(value));
  }

  return result;
}

template <typename RandomGenerator>
std::function<int()> GetValueGenerator(const ColumnDataDistribution& column_data_distribution,
                                       RandomGenerator& random_generator) {
  std::uniform_real_distribution probability_dist(0.0, 1.0);

  switch (column_data_distribution.distribution_type) {
    case DataDistributionType::kUniform: {
      const boost::math::uniform_distribution<double> uniform_dist(column_data_distribution.min_value,
                                                                   column_data_distribution.max_value);
      return [uniform_dist, probability_dist, &random_generator]() mutable {
        const auto probability = probability_dist(random_generator);
        return static_cast<int>(std::round(boost::math::quantile(uniform_dist, probability)));
      };
    }
    case DataDistributionType::kSkewedNormal: {
      const boost::math::skew_normal_distribution<double> skew_dist(column_data_distribution.skew_location,
                                                                    column_data_distribution.skew_scale,
                                                                    column_data_distribution.skew_shape);
      return [skew_dist, probability_dist, &random_generator]() mutable {
        const auto probability = probability_dist(random_generator);
        return static_cast<int>(std::round(boost::math::quantile(skew_dist, probability) * 10));
      };
    }
    case DataDistributionType::kPareto: {
      const boost::math::pareto_distribution<double> pareto_dist(column_data_distribution.pareto_scale,
                                                                 column_data_distribution.pareto_shape);
      return [pareto_dist, probability_dist, &random_generator]() mutable {
        const auto probability = probability_dist(random_generator);
        return static_cast<int>(std::round(boost::math::quantile(pareto_dist, probability)));
      };
    }
    default: {
      Fail("Unknown DataDistributionType.");
    }
  }
}

template <typename ColumnDataType>
std::shared_ptr<ValueSegment<ColumnDataType>> GenerateSegment(const ColumnSpecification& specification,
                                                              const size_t num_rows, const ChunkOffset chunk_size,
                                                              const ChunkId chunk_index) {
  auto random_generator = RandomGenerator<std::mt19937>();
  const auto generate_value = GetValueGenerator(specification.data_distribution, random_generator);

  /**
   * Generate values according to the distribution. We first add the given min and max values of that column to
   * avoid early exists via dictionary pruning (no matter which values are later searched, the local segment
   * dictionaries cannot prune them early). Therefore, we execute the value generation loop two times less.
   **/
  std::vector<int> values;
  values.reserve(chunk_size);

  values.push_back(static_cast<int>(specification.data_distribution.min_value));
  values.push_back(static_cast<int>(specification.data_distribution.max_value));

  for (size_t row_offset = 0; row_offset < chunk_size - 2; ++row_offset) {
    // If the number of remaining rows to insert is lower than the chunk size, this bound check ensures that only
    // num_rows values are inserted as opposed to num_chunks * chunk_size.
    if (static_cast<TableOffset>(chunk_index * chunk_size) + (row_offset + 1) > num_rows - 2) {
      break;
    }
    values.push_back(generate_value());
  }

  /**
   * If a ratio of to-be-created NULL values is given, fill the null_values vector used in the ValueSegment
   * constructor in a regular interval based on the null_ratio with true.
   */
  std::vector<bool> null_values;
  if (specification.null_ratio > 0.0f) {
    null_values.resize(chunk_size, false);

    const double step_size = 1.0 / specification.null_ratio;
    double current_row_offset = 0.0;
    while (current_row_offset < chunk_size) {
      null_values[static_cast<size_t>(std::round(current_row_offset))] = true;
      current_row_offset += step_size;
    }
  }

  if (specification.null_ratio > 0.0f) {
    return std::make_shared<ValueSegment<ColumnDataType>>(GenerateValues<ColumnDataType>(values),
                                                          std::move(null_values));
  } else {
    return std::make_shared<ValueSegment<ColumnDataType>>(GenerateValues<ColumnDataType>(values));
  }
}

}  // namespace

std::shared_ptr<Table> SyntheticTableGenerator::GenerateTable(const size_t num_columns, const size_t num_rows,
                                                              const ChunkOffset chunk_size) {
  const ColumnSpecification column_specification = {
      {ColumnDataDistribution::MakeUniformConfig(0.0, kMaxDifferentValues)}, DataType::kInt};
  return GenerateTable({num_columns, column_specification}, num_rows, chunk_size);
}

std::shared_ptr<Table> SyntheticTableGenerator::GenerateTable(
    const std::vector<ColumnSpecification>& column_specifications, const size_t num_rows,
    const ChunkOffset chunk_size) {
  Assert(chunk_size > 0, "Cannot generate table with chunk size 0 or less.");

  const size_t num_columns = column_specifications.size();
  const auto num_chunks = static_cast<size_t>(std::ceil(num_rows / static_cast<double>(chunk_size)));

  // Add column definitions and initialize each value vector.
  TableColumnDefinitions column_definitions;
  for (size_t column_id = 0; column_id < num_columns; ++column_id) {
    const std::string column_name = column_specifications[column_id].name
                                        ? column_specifications[column_id].name.value()
                                        : "column_" + std::to_string(column_id + 1);
    column_definitions.emplace_back(column_name, column_specifications[column_id].data_type, false);
  }

  auto table = std::make_shared<Table>(column_definitions);

  for (ChunkOffset chunk_index = 0; chunk_index < num_chunks; ++chunk_index) {
    Segments segments(num_columns);

    for (ChunkOffset column_index = 0; column_index < num_columns; ++column_index) {
      ResolveDataType(column_specifications[column_index].data_type, [&](auto column_data_type) {
        using ColumnDataType = decltype(column_data_type);

        segments[column_index] =
            GenerateSegment<ColumnDataType>(column_specifications[column_index], num_rows, chunk_size, chunk_index);
      });
    }

    table->AppendChunk(segments);
  }

  return table;
}

}  // namespace skyrise
