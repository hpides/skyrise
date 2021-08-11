#include "operator/import_operator.hpp"

#include <string_view>

#include <gtest/gtest.h>

#include "storage/backend/mock_storage.hpp"
#include "storage/backend/testdata_storage.hpp"
#include "storage/formats/csv_reader.hpp"
#include "storage/formats/mock_chunk_reader.hpp"
#include "storage/formats/orc_reader.hpp"
#include "storage/table/value_segment.hpp"
#include "types.hpp"

namespace skyrise {

class ImportOperatorTest : public ::testing::Test {
 public:
  using ValueSegmentGenerators = std::vector<ValueSegmentGenerator>;

  void SetUp() override {
    ValueSegmentGenerators segment_generators;
    const auto string_segment_generator_callback = []() {
      return std::make_shared<ValueSegment<std::string>>(std::vector<std::string>(kSizeSegments, "Test"));
    };
    segment_generators.emplace_back(string_segment_generator_callback);
    segment_generators.emplace_back(string_segment_generator_callback);
    segment_generators.emplace_back(string_segment_generator_callback);

    mock_schema_ = std::make_shared<TableColumnDefinitions>();
    mock_schema_->emplace_back("mock_schema", DataType::kString, false);
    mock_schema_->emplace_back("mock_schema_two", DataType::kString, false);
    mock_schema_->emplace_back("mock_schema_three", DataType::kString, false);

    mock_formatter_configuration_.num_chunks = kNumChunksMockFormatter;
    mock_formatter_configuration_.generators = segment_generators;
    mock_formatter_configuration_.schema = mock_schema_;
    mock_formatter_configuration_.error = false;

    orc_options_.parse_dates_as_string = true;

    csv_options_.has_header = true;
    csv_options_.has_types = true;

    mock_storage_ = std::make_shared<MockStorage>();

    test_data_storage_ = std::make_shared<TestdataStorage>();

    types_schema_ = std::make_shared<TableColumnDefinitions>();
    types_schema_->emplace_back("a_int", DataType::kInt, false);
    types_schema_->emplace_back("a_long", DataType::kLong, false);
    types_schema_->emplace_back("a_float", DataType::kFloat, false);
    types_schema_->emplace_back("a_double", DataType::kDouble, false);
    types_schema_->emplace_back("a_string", DataType::kString, false);
  }

  static void TestImportOperator(ImportOperator* import_operator, const TableColumnDefinitions& schema,
                                 size_t num_chunks, size_t row_count, std::vector<ColumnId>* included_column_ids) {
    import_operator->Execute();
    auto table = import_operator->GetOutput();

    ASSERT_NE(table, nullptr);
    EXPECT_EQ(table->ChunkCount(), num_chunks);
    EXPECT_EQ(table->RowCount(), row_count);

    std::optional<std::string> name = import_operator->Name();
    EXPECT_TRUE(name.has_value());

    std::sort(included_column_ids->begin(), included_column_ids->end());

    std::vector<ColumnId> pruned_column_ids;
    ExtractColumns(schema, *included_column_ids, &pruned_column_ids);
    for (const auto& column_id : pruned_column_ids) {
      EXPECT_ANY_THROW(table->ColumnIdByName(schema[column_id].name));
    }

    for (size_t counter = 0; counter < included_column_ids->size(); counter++) {
      EXPECT_EQ(schema[(*included_column_ids)[counter]].data_type, table->ColumnDataType(counter));
      EXPECT_EQ(schema[(*included_column_ids)[counter]].nullable, table->ColumnIsNullable(counter));
    }

    for (ChunkId chunk_counter = 0; chunk_counter < num_chunks; chunk_counter++) {
      const auto chunk = table->GetChunk(chunk_counter);
      EXPECT_EQ(chunk->GetColumnCount(), included_column_ids->size());
    }
  }

  static void ExtractColumns(const TableColumnDefinitions& schema, const std::vector<ColumnId>& included_column_ids,
                             std::vector<ColumnId>* pruned_column_ids) {
    auto iter = included_column_ids.begin();

    for (ColumnId column_id = 0; column_id < schema.size(); column_id++) {
      if (iter != included_column_ids.end() && column_id == *iter) {
        ++iter;
      } else {
        pruned_column_ids->push_back(column_id);
      }
    }
  }

  void SetupMockedImportOperator(std::vector<std::string>&& object_keys, std::vector<ColumnId>&& included_column_ids) {
    auto format_factory = std::make_shared<FormatReaderFactory<MockChunkReader>>(mock_formatter_configuration_);
    ImportOperator import_operator(mock_storage_, object_keys, included_column_ids, format_factory);

    const size_t num_chunks = kNumChunksMockFormatter * object_keys.size();
    const size_t row_count = kSizeSegments * num_chunks;
    TestImportOperator(&import_operator, *mock_schema_, num_chunks, row_count, &included_column_ids);
  }

 protected:
  static constexpr size_t kNumChunksMockFormatter = 3;
  static constexpr size_t kSizeSegments = 100;
  static constexpr std::string_view kCsvTypesPath = "csv/with_types.csv";
  static constexpr std::string_view kOrcTypesPath = "orc/with_types.orc";
  static constexpr std::string_view kOrcPartitionedPath = "orc/partitioned_int_string.orc";

  CsvFormatReaderOptions csv_options_;
  OrcFormatReaderOptions orc_options_;

  std::shared_ptr<TableColumnDefinitions> types_schema_;
  std::shared_ptr<TestdataStorage> test_data_storage_;

  MockChunkReaderConfiguration mock_formatter_configuration_;
  std::shared_ptr<MockStorage> mock_storage_;
  std::shared_ptr<TableColumnDefinitions> mock_schema_;
};

TEST_F(ImportOperatorTest, PrunedImport) {
  SetupMockedImportOperator({"a", "b"}, {ColumnId(0), ColumnId(1), ColumnId(2)});
  SetupMockedImportOperator({"a", "b"}, {ColumnId(0), ColumnId(2)});
  SetupMockedImportOperator({"a"}, {ColumnId(1)});
}

TEST_F(ImportOperatorTest, LargeIncludedColumnIdsList) {
  MockChunkReaderConfiguration formatter_configuration;
  formatter_configuration.num_chunks = 2;
  formatter_configuration.error = false;

  auto format_factory = std::make_shared<FormatReaderFactory<MockChunkReader>>(formatter_configuration);

  const std::vector<std::string> object_keys = {"a"};
  const std::vector<ColumnId> included_column_ids = {ColumnId(0), ColumnId(1), ColumnId(2), ColumnId(3)};
  ImportOperator import_operator(mock_storage_, object_keys, included_column_ids, format_factory);

  EXPECT_ANY_THROW(import_operator.Execute());
}

TEST_F(ImportOperatorTest, EmptyIncludedColumnIdsList) {
  MockChunkReaderConfiguration formatter_configuration;
  formatter_configuration.num_chunks = 2;
  formatter_configuration.error = false;

  auto format_factory = std::make_shared<FormatReaderFactory<MockChunkReader>>(formatter_configuration);

  const std::vector<std::string> object_keys = {"a"};
  ImportOperator import_operator(mock_storage_, object_keys, {}, format_factory);

  EXPECT_ANY_THROW(import_operator.Execute());
}

TEST_F(ImportOperatorTest, ImportEmptyChunks) {
  auto schema = std::make_shared<TableColumnDefinitions>();
  schema->emplace_back("test", DataType::kString, false);

  ValueSegmentGenerators segment_generators;
  const auto string_segment_generator_callback = []() {
    return std::make_shared<ValueSegment<std::string>>(std::vector<std::string>(0, "Test"));
  };
  segment_generators.emplace_back(string_segment_generator_callback);

  MockChunkReaderConfiguration formatter_configuration;
  formatter_configuration.num_chunks = 2;
  formatter_configuration.error = false;
  formatter_configuration.schema = schema;
  formatter_configuration.generators = segment_generators;

  auto format_factory = std::make_shared<FormatReaderFactory<MockChunkReader>>(formatter_configuration);

  const std::vector<std::string> object_keys = {"a"};
  std::vector<ColumnId> included_column_ids = {ColumnId(0)};
  ImportOperator import_operator(mock_storage_, object_keys, included_column_ids, format_factory);

  const size_t num_chunks = formatter_configuration.num_chunks * object_keys.size();
  const size_t row_count = 0;
  TestImportOperator(&import_operator, *schema, num_chunks, row_count, &included_column_ids);
}

TEST_F(ImportOperatorTest, ImportCsv) {
  auto csv_factory = std::make_shared<FormatReaderFactory<CsvFormatReader>>(csv_options_);

  const std::vector<std::string> object_keys = {kCsvTypesPath.data()};
  std::vector<ColumnId> included_column_ids = {ColumnId(0), ColumnId(1), ColumnId(3), ColumnId(4)};
  ImportOperator import_operator(test_data_storage_, object_keys, included_column_ids, csv_factory);

  TestImportOperator(&import_operator, *types_schema_, 1, 1, &included_column_ids);
}

TEST_F(ImportOperatorTest, ImportOrc) {
  auto orc_factory = std::make_shared<FormatReaderFactory<OrcFormatReader>>(orc_options_);

  const std::vector<std::string> object_keys = {kOrcTypesPath.data()};
  std::vector<ColumnId> included_column_ids = {ColumnId(0), ColumnId(1), ColumnId(2), ColumnId(3), ColumnId(4)};
  ImportOperator import_operator(test_data_storage_, object_keys, included_column_ids, orc_factory);

  TestImportOperator(&import_operator, *types_schema_, 1, 1, &included_column_ids);
}

TEST_F(ImportOperatorTest, ImportPartitionedOrc) {
  auto schema = std::make_shared<TableColumnDefinitions>();
  schema->emplace_back("a", DataType::kInt, false);
  schema->emplace_back("b", DataType::kString, false);

  OrcFormatReaderOptions orc_options;
  orc_options.expected_schema = schema;
  orc_options.select_partition_range = std::make_pair(2, 3);

  auto orc_factory = std::make_shared<FormatReaderFactory<OrcFormatReader>>(orc_options);

  const std::vector<std::string> object_keys = {kOrcPartitionedPath.data()};
  std::vector<ColumnId> included_column_ids = {ColumnId(0), ColumnId(1)};
  ImportOperator import_operator(test_data_storage_, object_keys, included_column_ids, orc_factory);

  TestImportOperator(&import_operator, *schema, 1, 20, &included_column_ids);
}

}  // namespace skyrise
