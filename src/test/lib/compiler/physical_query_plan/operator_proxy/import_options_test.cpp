#include "compiler/physical_query_plan/operator_proxy/import_options.hpp"

#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "expression/binary_predicate_expression.hpp"
#include "expression/value_expression.hpp"
#include "storage/formats/csv_reader.hpp"
#include "storage/formats/orc_reader.hpp"
#include "types.hpp"

namespace skyrise {

class ImportOptionsTest : public ::testing::Test {
 public:
  void SetUp() override {
    column_definitions_a_ = std::make_shared<TableColumnDefinitions>();
    column_definitions_a_->emplace_back("a", DataType::kInt, false);
    column_definitions_x_y_ = std::make_shared<TableColumnDefinitions>();
    column_definitions_x_y_->emplace_back("x", DataType::kInt, false);
    column_definitions_x_y_->emplace_back("y", DataType::kDouble, false);

    csv_options_.expected_schema = column_definitions_a_;
    csv_options_.read_buffer_size = 100_MB;
    csv_options_.delimiter = '!';
    csv_options_.guess_delimiter = false;
    csv_options_.guess_has_header = false;
    csv_options_.guess_has_types = false;
    csv_options_.has_header = true;
    csv_options_.has_types = true;

    orc_options_.parse_dates_as_string = false;
  }

 protected:
  CsvFormatReaderOptions csv_options_;
  OrcFormatReaderOptions orc_options_;
  std::shared_ptr<TableColumnDefinitions> column_definitions_a_;
  std::shared_ptr<TableColumnDefinitions> column_definitions_x_y_;
};

TEST_F(ImportOptionsTest, CreateReaderFactoryCsvDefaultOptions) {
  const auto import_options = std::make_shared<ImportOptions>(ImportFormat::kCsv);
  const auto reader_factory = import_options->CreateReaderFactory();
  ASSERT_NE(reader_factory, nullptr);
  const auto csv_reader_factory = std::dynamic_pointer_cast<FormatReaderFactory<CsvFormatReader>>(reader_factory);
  ASSERT_NE(csv_reader_factory, nullptr);
  EXPECT_EQ(static_cast<const CsvFormatReaderOptions&>(csv_reader_factory->Configuration()), CsvFormatReaderOptions());
}

TEST_F(ImportOptionsTest, CreateReaderFactoryCsvCustomOptions) {
  const auto import_options = std::make_shared<ImportOptions>(csv_options_);
  const auto reader_factory = import_options->CreateReaderFactory();
  const auto csv_reader_factory = std::dynamic_pointer_cast<FormatReaderFactory<CsvFormatReader>>(reader_factory);
  ASSERT_NE(csv_reader_factory, nullptr);
  EXPECT_EQ(static_cast<const CsvFormatReaderOptions&>(csv_reader_factory->Configuration()), csv_options_);
}

TEST_F(ImportOptionsTest, CreateReaderFactoryOrcDefaultOptions) {
  const auto import_options = std::make_shared<ImportOptions>(ImportFormat::kOrc);
  const auto reader_factory = import_options->CreateReaderFactory();
  ASSERT_NE(reader_factory, nullptr);
  const auto orc_reader_factory = std::dynamic_pointer_cast<FormatReaderFactory<OrcFormatReader>>(reader_factory);
  ASSERT_TRUE(orc_reader_factory);
  EXPECT_EQ(static_cast<const OrcFormatReaderOptions&>(orc_reader_factory->Configuration()), OrcFormatReaderOptions());
}

TEST_F(ImportOptionsTest, SerializeAndDeserializeTableColumnDefinitions) {
  Aws::Utils::Json::JsonValue json_value;

  // (1) Serialize
  json_value.WithArray("column_definitions_a", ImportOptions::TableColumnDefinitionsToJsonArray(column_definitions_a_));
  json_value.WithArray("column_definitions_x_y",
                       ImportOptions::TableColumnDefinitionsToJsonArray(column_definitions_x_y_));
  const auto json_view = json_value.View();

  // (2) Deserialize & verify
  const auto column_definitions_a =
      ImportOptions::TableColumnDefinitionsFromJsonArray(json_view.GetArray("column_definitions_a"));
  const auto column_definitions_x_y =
      ImportOptions::TableColumnDefinitionsFromJsonArray(json_view.GetArray("column_definitions_x_y"));
  EXPECT_NE(*column_definitions_a, *column_definitions_x_y);
  EXPECT_EQ(*column_definitions_a, *column_definitions_a_);
  EXPECT_EQ(*column_definitions_x_y, *column_definitions_x_y_);
}

TEST_F(ImportOptionsTest, IncludeColumns) {
  const std::vector<ColumnId> include_columns = {0, 1, 3};

  EXPECT_NO_THROW(std::make_shared<ImportOptions>(ImportFormat::kCsv, include_columns));

  const auto import_options = std::make_shared<ImportOptions>(ImportFormat::kOrc, include_columns);

  const auto reader_factory_a = import_options->CreateReaderFactory();
  const auto orc_reader_factory_a = std::dynamic_pointer_cast<FormatReaderFactory<OrcFormatReader>>(reader_factory_a);
  EXPECT_TRUE(orc_reader_factory_a->Configuration().include_columns.has_value());
  EXPECT_EQ(orc_reader_factory_a->Configuration().include_columns.value(), include_columns);

  const auto serialized_json = import_options->ToJson();
  const auto deserialized_import_options = ImportOptions::FromJson(serialized_json);

  const auto reader_factory_b = deserialized_import_options->CreateReaderFactory();
  const auto orc_reader_factory_b = std::dynamic_pointer_cast<FormatReaderFactory<OrcFormatReader>>(reader_factory_b);
  EXPECT_TRUE(orc_reader_factory_b->Configuration().include_columns.has_value());
  EXPECT_EQ(orc_reader_factory_b->Configuration().include_columns.value(), include_columns);
}

TEST_F(ImportOptionsTest, CreateReaderFactoryParquetCustomOptions) {
  // ParquetOptions with BinaryPredicateExpression
  ParquetFormatReaderOptions parquet_format_reader_options;

  parquet_format_reader_options.skyrise_expression = std::make_optional(std::make_shared<BinaryPredicateExpression>(
      PredicateCondition::kEquals, std::make_shared<ValueExpression>(1), std::make_shared<ValueExpression>(1)));
  const auto arrow_expression = arrow::compute::equal(arrow::compute::literal(1), arrow::compute::literal(1));
  const auto import_options = std::make_shared<ImportOptions>(parquet_format_reader_options);
  const auto reader_factory = import_options->CreateReaderFactory();
  const auto parquet_reader_factory =
      std::dynamic_pointer_cast<FormatReaderFactory<ParquetFormatReader>>(reader_factory);
  ASSERT_NE(parquet_reader_factory, nullptr);

  // Serialize / Deserialize
  const auto serialized_json = import_options->ToJson();
  const auto deserialized_import_options = ImportOptions::FromJson(serialized_json);
  const auto reader_factory_b = deserialized_import_options->CreateReaderFactory();
  const auto parquet_reader_factory_b =
      std::dynamic_pointer_cast<FormatReaderFactory<ParquetFormatReader>>(reader_factory_b);
  ASSERT_NE(parquet_reader_factory_b, nullptr);
  ASSERT_TRUE(parquet_reader_factory_b->Configuration().arrow_expression.has_value());
  EXPECT_EQ(arrow_expression, parquet_reader_factory_b->Configuration().arrow_expression);
}

TEST_F(ImportOptionsTest, CreateReaderFactoryParquetRowGroupIds) {
  ParquetFormatReaderOptions parquet_format_reader_options;
  std::vector<int32_t> row_group_ids = {0, 1, 2, 3};

  parquet_format_reader_options.row_group_ids = row_group_ids;
  const auto import_options = std::make_shared<ImportOptions>(parquet_format_reader_options);
  const auto reader_factory = import_options->CreateReaderFactory();
  const auto parquet_reader_factory =
      std::dynamic_pointer_cast<FormatReaderFactory<ParquetFormatReader>>(reader_factory);
  ASSERT_NE(parquet_reader_factory, nullptr);

  // Serialize / Deserialize
  const auto serialized_json = import_options->ToJson();
  const auto deserialized_import_options = ImportOptions::FromJson(serialized_json);
  const auto reader_factory_b = deserialized_import_options->CreateReaderFactory();
  const auto parquet_reader_factory_b =
      std::dynamic_pointer_cast<FormatReaderFactory<ParquetFormatReader>>(reader_factory_b);
  ASSERT_NE(parquet_reader_factory_b, nullptr);
  ASSERT_TRUE(parquet_reader_factory_b->Configuration().row_group_ids.has_value());
  EXPECT_EQ(row_group_ids, parquet_reader_factory_b->Configuration().row_group_ids);
}

}  // namespace skyrise
