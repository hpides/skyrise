/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#pragma once

#include <cmath>
#include <memory>
#include <optional>

#include "all_type_variant.hpp"
#include "storage/storage_types.hpp"
#include "storage/table/table.hpp"
#include "utils/string.hpp"

namespace skyrise {

enum class DataDistributionType { kUniform, kSkewedNormal, kPareto };

struct ColumnDataDistribution {
  static ColumnDataDistribution MakeUniformConfig(const double min, const double max) {
    ColumnDataDistribution column_data_distribution;
    column_data_distribution.min_value = min;
    column_data_distribution.max_value = max;
    column_data_distribution.num_different_values = static_cast<int>(std::floor(max - min));
    return column_data_distribution;
  }

  static ColumnDataDistribution MakeSkewedNormalConfig(const double skew_location = 0.0, const double skew_scale = 1.0,
                                                       const double skew_shape = 0.0) {
    ColumnDataDistribution column_data_distribution;
    column_data_distribution.skew_location = skew_location;
    column_data_distribution.skew_scale = skew_scale;
    column_data_distribution.skew_shape = skew_shape;
    column_data_distribution.distribution_type = DataDistributionType::kSkewedNormal;
    return column_data_distribution;
  }

  static ColumnDataDistribution MakeParetoConfig(const double pareto_scale = 1.0, const double pareto_shape = 1.0) {
    ColumnDataDistribution column_data_distribution;
    column_data_distribution.pareto_scale = pareto_scale;
    column_data_distribution.pareto_shape = pareto_shape;
    column_data_distribution.distribution_type = DataDistributionType::kPareto;
    return column_data_distribution;
  }

  DataDistributionType distribution_type = DataDistributionType::kUniform;

  int num_different_values = 1'000;

  double pareto_scale = 0;
  double pareto_shape = 0;

  double skew_location = 0;
  double skew_scale = 0;
  double skew_shape = 0;

  double min_value = 0;
  double max_value = 0;
};

struct ColumnSpecification {
  ColumnSpecification(const ColumnDataDistribution& init_data_distribution, const DataType& init_data_type,
                      const std::optional<std::string>& init_name = std::nullopt, const float init_null_ratio = 0.0f)
      : data_distribution(init_data_distribution),
        data_type(init_data_type),
        name(init_name),
        null_ratio(init_null_ratio) {}

  const ColumnDataDistribution data_distribution;
  const DataType data_type;
  const std::optional<std::string> name;
  const float null_ratio;
};

class SyntheticTableGenerator {
 public:
  // Simple table generation, mainly for simple tests.
  static std::shared_ptr<Table> GenerateTable(const size_t num_columns, const size_t num_rows,
                                              const ChunkOffset chunk_size = kChunkDefaultSize);

  static std::shared_ptr<Table> GenerateTable(const std::vector<ColumnSpecification>& column_specifications,
                                              const size_t num_rows, const ChunkOffset chunk_size = kChunkDefaultSize);

  /**
   * Function to create a typed value from an integer. The data generation creates integers with the requested
   * distribution and this function is used to create different types. The creation should guarantee that searching
   * for the integer value 100 within a column of the range (0,200) should return half of all tuples, not only for
   * ints but also for all other value types such as strings.
   * Handling of values:
   *   - in case of long, the integer is simply casted
   *   - in case of floating types, the integer is slightly modified and casted to ensure a mantissa that is not fully
   *     zero'd. The modification adds noise to make the floating point values in some sense more realistic.
   *   - in case of strings, a 10 char string is created that has at least `kStringPrefixLength` leading spaces to
   *     ensure that scans have to evaluate at least the first four chars. The reason is that very often strings are
   *     dates in in ERP systems and we assume that at least the year has to be read before a non-match can be
   *     determined. Randomized strings often lead to unrealistically fast string scans. An example would be randomized
   *     strings in which a typical linear scan only has to read the first char in order to disqualify a tuple. Strings
   *     in real world systems often share a common prefix (e.g., country code prefixes or dates starting with the year)
   *     where usually more chars need to be read. The observed effect was that operations on randomized strings were
   *     unexpectedly faster than seen with real-world data.
   *     Example (shortening leading spaces): (0, 1, 2, 10, 11, 61, 62, 75) >>
   *                                          ('   ', '  1', '  2', '  A', '  B', '  z', ' 10', ' 11', ' 1D')
   */
  template <typename T>
  static T GenerateValue(const int input) {
    if constexpr (std::is_integral_v<T>) {
      return static_cast<T>(input);
    } else if constexpr (std::is_floating_point_v<T>) {
      return static_cast<T>(input) * 0.999999f;
    } else if constexpr (std::is_same_v<T, std::string>) {
      Assert(input >= 0, "Integer values need to be positive in order to be converted to a string.");
      Assert(static_cast<double>(input) < std::pow(kValidStringChars.size(), kVariableStringLength),
             "Input too large. Cannot be represented in " + std::to_string(kVariableStringLength) + " chars.");

      std::string result(kGeneratedStringLength, ' ');  // fill full length with spaces
      if (input == 0) {
        return result;
      }

      const auto result_char_count =
          static_cast<size_t>(std::floor(std::log(input) / std::log(kValidStringChars.size())) + 1);
      auto remainder = static_cast<size_t>(input);
      for (size_t i = 0; i < result_char_count; ++i) {
        result[kGeneratedStringLength - 1 - i] = kValidStringChars[remainder % kValidStringChars.size()];
        remainder = remainder / kValidStringChars.size();
      }

      return result;
    }
  }

 protected:
  static constexpr size_t kMaxDifferentValues = 10'000;
  static constexpr size_t kGeneratedStringLength = 10;
  static constexpr size_t kStringPrefixLength = 4;
  static constexpr size_t kVariableStringLength = kGeneratedStringLength - kStringPrefixLength;

  static inline const std::string kValidStringChars = kCharacterSetDecimal + kCharacterSetUpper + kCharacterSetLower;
};

}  // namespace skyrise
