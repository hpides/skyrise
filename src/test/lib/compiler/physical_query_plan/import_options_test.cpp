#include "compiler/physical_query_plan/import_options.hpp"

#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "storage/formats/csv_reader.hpp"
#include "storage/formats/orc_reader.hpp"
#include "storage/table/chunk_reader.hpp"
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

    orc_options_partition_range_.parse_dates_as_string = true;
    orc_options_partition_range_.expected_schema = column_definitions_a_;
    orc_options_partition_range_.select_partition_range = std::make_pair(5, 8);

    orc_options_row_range_.parse_dates_as_string = false;
    orc_options_row_range_.expected_schema = column_definitions_x_y_;
    orc_options_row_range_.select_row_range = std::make_pair(50, 100);
  }

 protected:
  CsvFormatReaderOptions csv_options_;
  OrcFormatReaderOptions orc_options_partition_range_;
  OrcFormatReaderOptions orc_options_row_range_;
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
  auto import_options = std::make_shared<ImportOptions>(csv_options_);
  auto reader_factory = import_options->CreateReaderFactory();
  auto csv_reader_factory = std::dynamic_pointer_cast<FormatReaderFactory<CsvFormatReader>>(reader_factory);
  ASSERT_NE(csv_reader_factory, nullptr);
  EXPECT_EQ(static_cast<const CsvFormatReaderOptions&>(csv_reader_factory->Configuration()), csv_options_);
}

TEST_F(ImportOptionsTest, CreateReaderFactoryOrcDefaultOptions) {
  auto import_options = std::make_shared<ImportOptions>(ImportFormat::kOrc);
  auto reader_factory = import_options->CreateReaderFactory();
  ASSERT_NE(reader_factory, nullptr);
  const auto orc_reader_factory = std::dynamic_pointer_cast<FormatReaderFactory<OrcFormatReader>>(reader_factory);
  ASSERT_TRUE(orc_reader_factory);
  EXPECT_EQ(static_cast<const OrcFormatReaderOptions&>(orc_reader_factory->Configuration()), OrcFormatReaderOptions());
}

TEST_F(ImportOptionsTest, CreateReaderFactoryOrcCustomOptions) {
  {
    // OrcOptions specifying a partition range
    auto import_options = std::make_shared<ImportOptions>(orc_options_partition_range_);
    auto reader_factory = import_options->CreateReaderFactory();
    auto orc_reader_factory = std::dynamic_pointer_cast<FormatReaderFactory<OrcFormatReader>>(reader_factory);
    ASSERT_NE(orc_reader_factory, nullptr);
    EXPECT_EQ(static_cast<const OrcFormatReaderOptions&>(orc_reader_factory->Configuration()),
              orc_options_partition_range_);
  }
  {
    // OrcOptions specifying a row range
    auto import_options = std::make_shared<ImportOptions>(orc_options_row_range_);
    auto reader_factory = import_options->CreateReaderFactory();
    auto orc_reader_factory = std::dynamic_pointer_cast<FormatReaderFactory<OrcFormatReader>>(reader_factory);
    ASSERT_NE(orc_reader_factory, nullptr);
    EXPECT_EQ(static_cast<const OrcFormatReaderOptions&>(orc_reader_factory->Configuration()), orc_options_row_range_);
  }
}

TEST_F(ImportOptionsTest, SerializeAndDeserializeTableColumnDefinitions) {
  Aws::Utils::Json::JsonValue json_value;

  // (1) Serialize
  json_value.WithArray("column_definitions_a", ImportOptions::TableColumnDefinitionsToJsonArray(column_definitions_a_));
  json_value.WithArray("column_definitions_x_y",
                       ImportOptions::TableColumnDefinitionsToJsonArray(column_definitions_x_y_));
  auto json_view = json_value.View();

  // (2) Deserialize & verify
  auto column_definitions_a =
      ImportOptions::TableColumnDefinitionsFromJsonArray(json_view.GetArray("column_definitions_a"));
  auto column_definitions_x_y =
      ImportOptions::TableColumnDefinitionsFromJsonArray(json_view.GetArray("column_definitions_x_y"));
  EXPECT_NE(*column_definitions_a, *column_definitions_x_y);
  EXPECT_EQ(*column_definitions_a, *column_definitions_a_);
  EXPECT_EQ(*column_definitions_x_y, *column_definitions_x_y_);
}

TEST_F(ImportOptionsTest, SerializeAndDeserializeOrcOptions) {
  {
    // OrcOptions specifying a partition range
    Aws::Utils::Json::JsonValue json;
    auto import_options = std::make_shared<ImportOptions>(orc_options_partition_range_);

    //  (1) Serialize
    json.WithObject("import_options", import_options->ToJson());
    auto json_view = json.View();

    //  (2) Deserialize & verify attributes
    auto deserialized_import_options = ImportOptions::FromJson(json_view.GetObject("import_options"));
    auto reader_factory = deserialized_import_options->CreateReaderFactory();
    auto orc_reader_factory = std::dynamic_pointer_cast<FormatReaderFactory<OrcFormatReader>>(reader_factory);
    ASSERT_NE(orc_reader_factory, nullptr);
    EXPECT_EQ(static_cast<const OrcFormatReaderOptions&>(orc_reader_factory->Configuration()),
              orc_options_partition_range_);

    //  (3) Serialize again
    Aws::Utils::Json::JsonValue deserialized_json;
    deserialized_json.WithObject("import_options", deserialized_import_options->ToJson());
    EXPECT_EQ(json, deserialized_json);
  }
  {
    // OrcOptions specifying a row range
    Aws::Utils::Json::JsonValue json;
    auto import_options = std::make_shared<ImportOptions>(orc_options_row_range_);

    //  (1) Serialize
    json.WithObject("import_options", import_options->ToJson());
    auto json_view = json.View();

    //  (2) Deserialize & verify attributes
    auto deserialized_import_options = ImportOptions::FromJson(json_view.GetObject("import_options"));
    auto reader_factory = deserialized_import_options->CreateReaderFactory();
    auto orc_reader_factory = std::dynamic_pointer_cast<FormatReaderFactory<OrcFormatReader>>(reader_factory);
    ASSERT_NE(orc_reader_factory, nullptr);
    EXPECT_EQ(static_cast<const OrcFormatReaderOptions&>(orc_reader_factory->Configuration()), orc_options_row_range_);

    //  (3) Serialize again
    Aws::Utils::Json::JsonValue deserialized_json;
    deserialized_json.WithObject("import_options", deserialized_import_options->ToJson());
    EXPECT_EQ(json, deserialized_json);
  }
}

}  // namespace skyrise
