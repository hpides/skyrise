#include "component_benchmark/synthetic_table_generator.hpp"

#include <gtest/gtest.h>
#include <magic_enum/magic_enum.hpp>

#include "storage/table/value_segment.hpp"

namespace skyrise {

TEST(SyntheticTableGeneratorTest, StringGeneration) {
  EXPECT_EQ(SyntheticTableGenerator::GenerateValue<std::string>(0), "          ");
  EXPECT_EQ(SyntheticTableGenerator::GenerateValue<std::string>(1), "         1");
  EXPECT_EQ(SyntheticTableGenerator::GenerateValue<std::string>(2), "         2");
  EXPECT_EQ(SyntheticTableGenerator::GenerateValue<std::string>(17), "         H");
  EXPECT_EQ(SyntheticTableGenerator::GenerateValue<std::string>(117), "        1t");
  EXPECT_EQ(SyntheticTableGenerator::GenerateValue<std::string>(50'018), "       D0k");
  EXPECT_EQ(SyntheticTableGenerator::GenerateValue<std::string>(3'433'820), "      EPIC");

  // Negative values are not supported.
  ASSERT_THROW(SyntheticTableGenerator::GenerateValue<std::string>(-1), std::logic_error);
}

TEST(SyntheticTableGeneratorTest, TestGeneratedValueRange) {
  const size_t row_count = 105;
  const size_t chunk_size = 10;
  const auto table_generator = std::make_shared<SyntheticTableGenerator>();
  const auto uniform_distribution_0_10 = ColumnDataDistribution::MakeUniformConfig(0.0, 10.0);

  const auto table = table_generator->GenerateTable({ColumnSpecification(uniform_distribution_0_10, DataType::kDouble)},
                                                    row_count, chunk_size);

  EXPECT_EQ(table->RowCount(), row_count);
  EXPECT_EQ(table->ChunkCount(), std::ceil(row_count / static_cast<double>(chunk_size)));

  std::set<double> unique_values;
  for (size_t i = 0; i < table->ChunkCount() - 1; i++) {
    EXPECT_EQ(table->GetChunk(i)->Size(), chunk_size);
    const auto segment = std::dynamic_pointer_cast<ValueSegment<double>>(table->GetChunk(i)->GetSegment(0));
    unique_values.insert(segment->Values().begin(), segment->Values().end());
  }
  // Min and Max values are added by default. Checking >=3 makes sure there are other values.
  EXPECT_GE(unique_values.size(), 3);

  const size_t last_chunk_row_count = row_count % chunk_size;
  if (last_chunk_row_count > 0) {
    EXPECT_EQ(table->LastChunk()->Size(), last_chunk_row_count);
  }
}

using TestParameters = std::tuple<DataType, ColumnDataDistribution>;

class SyntheticTableGeneratorDataTypeTests : public testing::TestWithParam<TestParameters> {};

// NOLINTNEXTLINE(cppcoreguidelines-special-member-functions,hicpp-special-member-functions)
TEST_P(SyntheticTableGeneratorDataTypeTests, IntegerTable) {
  const size_t row_count = 25;
  const size_t chunk_size = 10;

  const auto tested_data_type = std::get<0>(GetParam());
  auto table_generator = std::make_shared<SyntheticTableGenerator>();

  const std::vector<ColumnSpecification> column_specifications{
      {std::get<1>(GetParam()), tested_data_type, "column_name"}};

  auto table = table_generator->GenerateTable(column_specifications, row_count, chunk_size);

  const auto generated_chunk_count = table->ChunkCount();
  const auto generated_column_count = table->GetColumnCount();
  EXPECT_EQ(table->RowCount(), row_count);
  EXPECT_EQ(generated_chunk_count, static_cast<size_t>(std::round(static_cast<float>(row_count) / chunk_size)));
  EXPECT_EQ(generated_column_count, column_specifications.size());

  for (ChunkOffset column_id = 0; column_id < generated_column_count; ++column_id) {
    EXPECT_EQ(table->ColumnDataType(column_id), tested_data_type);
    EXPECT_EQ(table->ColumnName(column_id), "column_name");
  }
}

auto formatter = [](const testing::TestParamInfo<TestParameters> info) {
  std::stringstream stream;
  switch (std::get<1>(info.param).distribution_type) {
    case DataDistributionType::kUniform:
      stream << "kUniform";
      break;
    case DataDistributionType::kSkewedNormal:
      stream << "kSkewedNormal";
      break;
    case DataDistributionType::kPareto:
      stream << "kPareto";
  }

  stream << "_" << magic_enum::enum_name(std::get<0>(info.param));
  return stream.str();
};

// For the skewed distribution, we use a location of 1,000 to move the distribution far into the positive number range.
// The reason is that string values cannot be generated for negative values.
INSTANTIATE_TEST_SUITE_P(SyntheticTableGeneratorDataType, SyntheticTableGeneratorDataTypeTests,
                         testing::Combine(testing::Values(DataType::kInt, DataType::kLong, DataType::kFloat,
                                                          DataType::kDouble, DataType::kString),
                                          testing::Values(ColumnDataDistribution::MakeUniformConfig(0.0, 10'000),
                                                          ColumnDataDistribution::MakeParetoConfig(),
                                                          ColumnDataDistribution::MakeSkewedNormalConfig(1'000.0))),
                         formatter);
}  // namespace skyrise
