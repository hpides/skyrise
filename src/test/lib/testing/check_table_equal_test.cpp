#include "testing/check_table_equal.hpp"

#include <gtest/gtest.h>

#include "storage/table/table.hpp"
#include "storage/table/value_segment.hpp"

namespace skyrise {

class TableVerificationTest : public ::testing::Test {
  using Chunks = std::vector<std::shared_ptr<Chunk>>;

  void SetUp() override {
    expected_schema_ = GenerateAllTypeColumnDefinition();
    equal_schema_ = GenerateAllTypeColumnDefinition();

    type_mismatch_schema_ = GenerateAllTypeColumnDefinition();
    type_mismatch_schema_[0].data_type = DataType::kFloat;

    name_mismatch_schema_ = GenerateAllTypeColumnDefinition();
    name_mismatch_schema_[0].name = "error";

    nullable_mismatch_schema_ = GenerateAllTypeColumnDefinition();
    nullable_mismatch_schema_[0].nullable = true;

    expected_chunks_ = GenerateValues();
    equal_chunks_ = GenerateValues();

    std::vector<float> new_value_float{458.7F, 45.7F, -1.0F};
    float_mismatch_chunks_ = GenerateValues();
    float_mismatch_chunks_[0]->ReplaceSegment(2, std::make_shared<ValueSegment<float>>(std::move(new_value_float)));

    std::vector<double> new_value_double{458, 46.7, -1.0};
    double_mismatch_chunks_ = GenerateValues();
    double_mismatch_chunks_[0]->ReplaceSegment(3, std::make_shared<ValueSegment<double>>(std::move(new_value_double)));

    std::vector<std::string> new_value_string{"first", "second", "mismatch"};
    string_mismatch_chunks_ = GenerateValues();
    string_mismatch_chunks_[0]->ReplaceSegment(
        4, std::make_shared<ValueSegment<std::string>>(std::move(new_value_string)));
  }

 protected:
  static TableColumnDefinitions GenerateAllTypeColumnDefinition() {
    auto table_definitions = TableColumnDefinitions();
    table_definitions.emplace_back("L_ORDERKEY", DataType::kLong, false);
    table_definitions.emplace_back("L_SUPPKEY", DataType::kInt, false);
    table_definitions.emplace_back("L_EXTENDEDPRICE", DataType::kFloat, false);
    table_definitions.emplace_back("test", DataType::kDouble, false);
    table_definitions.emplace_back("L_RETURNFLAG", DataType::kString, false);
    return table_definitions;
  }

  static Chunks GenerateValues() {
    std::vector<int64_t> long_values{1235, 123231, 124};
    std::vector<int32_t> int_values{12345, 123, 1234};
    std::vector<float> float_values{458.7F, 45.7F, 57.7F};
    std::vector<double> double_values{458, 46.7, 47.7};
    std::vector<std::string> string_values{"first", "second", "third"};

    Segments segments;
    segments.push_back(std::make_shared<ValueSegment<int64_t>>(std::move(long_values)));
    segments.push_back(std::make_shared<ValueSegment<int32_t>>(std::move(int_values)));
    segments.push_back(std::make_shared<ValueSegment<float>>(std::move(float_values)));
    segments.push_back(std::make_shared<ValueSegment<double>>(std::move(double_values)));
    segments.push_back(std::make_shared<ValueSegment<std::string>>(std::move(string_values)));

    Chunks chunks;
    chunks.push_back(std::make_shared<Chunk>(std::move(segments)));

    return chunks;
  }

  TableColumnDefinitions expected_schema_, equal_schema_, name_mismatch_schema_, type_mismatch_schema_,
      nullable_mismatch_schema_;

  Chunks expected_chunks_, equal_chunks_, float_mismatch_chunks_, double_mismatch_chunks_, string_mismatch_chunks_,
      int_mismatch_chunks_;
};

TEST_F(TableVerificationTest, EqualSchema) {
  const auto actual_table = std::make_shared<Table>(equal_schema_);
  const auto expected_table = std::make_shared<Table>(expected_schema_);

  EXPECT_FALSE(
      CheckTableEqual(actual_table, expected_table, OrderSensitivity::kNo, FloatComparisonMode::kAbsoluteDifference));
}

TEST_F(TableVerificationTest, NotEqualSchemaType) {
  const auto actual_table = std::make_shared<Table>(type_mismatch_schema_);
  const auto expected_table = std::make_shared<Table>(expected_schema_);

  EXPECT_TRUE(
      CheckTableEqual(actual_table, expected_table, OrderSensitivity::kNo, FloatComparisonMode::kAbsoluteDifference));
}

TEST_F(TableVerificationTest, NotEqualSchemaName) {
  const auto actual_table = std::make_shared<Table>(name_mismatch_schema_);
  const auto expected_table = std::make_shared<Table>(expected_schema_);

  EXPECT_TRUE(
      CheckTableEqual(actual_table, expected_table, OrderSensitivity::kNo, FloatComparisonMode::kAbsoluteDifference));
}

TEST_F(TableVerificationTest, NotEqualSchemaNullable) {
  const auto actual_table = std::make_shared<Table>(nullable_mismatch_schema_);
  const auto expected_table = std::make_shared<Table>(expected_schema_);

  EXPECT_TRUE(
      CheckTableEqual(actual_table, expected_table, OrderSensitivity::kNo, FloatComparisonMode::kAbsoluteDifference));
}

TEST_F(TableVerificationTest, EqualTableValues) {
  const auto actual_table = std::make_shared<Table>(equal_schema_, std::move(equal_chunks_));
  const auto expected_table = std::make_shared<Table>(expected_schema_, std::move(expected_chunks_));

  EXPECT_FALSE(
      CheckTableEqual(actual_table, expected_table, OrderSensitivity::kNo, FloatComparisonMode::kAbsoluteDifference));
}

TEST_F(TableVerificationTest, LargeEqualTableValues) {
  auto extend = GenerateValues();
  equal_chunks_.insert(equal_chunks_.end(), extend.begin(), extend.end());

  auto extend_two = GenerateValues();
  expected_chunks_.insert(expected_chunks_.end(), extend_two.begin(), extend_two.end());

  const auto actual_table = std::make_shared<Table>(equal_schema_, std::move(equal_chunks_));
  const auto expected_table = std::make_shared<Table>(expected_schema_, std::move(expected_chunks_));

  EXPECT_FALSE(
      CheckTableEqual(actual_table, expected_table, OrderSensitivity::kNo, FloatComparisonMode::kAbsoluteDifference));
}

TEST_F(TableVerificationTest, NotEqualTableValuesFloat) {
  const auto actual_table = std::make_shared<Table>(equal_schema_, std::move(float_mismatch_chunks_));
  const auto expected_table = std::make_shared<Table>(expected_schema_, std::move(expected_chunks_));

  EXPECT_TRUE(
      CheckTableEqual(actual_table, expected_table, OrderSensitivity::kNo, FloatComparisonMode::kAbsoluteDifference));
}

TEST_F(TableVerificationTest, NotEqualTableValuesInt) {
  const auto actual_table = std::make_shared<Table>(equal_schema_, std::move(int_mismatch_chunks_));
  const auto expected_table = std::make_shared<Table>(expected_schema_, std::move(expected_chunks_));

  EXPECT_TRUE(
      CheckTableEqual(actual_table, expected_table, OrderSensitivity::kNo, FloatComparisonMode::kAbsoluteDifference));
}

TEST_F(TableVerificationTest, NotEqualTableValuesDouble) {
  const auto actual_table = std::make_shared<Table>(equal_schema_, std::move(double_mismatch_chunks_));
  const auto expected_table = std::make_shared<Table>(expected_schema_, std::move(expected_chunks_));

  EXPECT_TRUE(
      CheckTableEqual(actual_table, expected_table, OrderSensitivity::kNo, FloatComparisonMode::kAbsoluteDifference));
}

TEST_F(TableVerificationTest, NotEqualTableValuesString) {
  const auto actual_table = std::make_shared<Table>(equal_schema_, std::move(string_mismatch_chunks_));
  const auto expected_table = std::make_shared<Table>(expected_schema_, std::move(expected_chunks_));

  EXPECT_TRUE(
      CheckTableEqual(actual_table, expected_table, OrderSensitivity::kNo, FloatComparisonMode::kAbsoluteDifference));
}

}  // namespace skyrise
