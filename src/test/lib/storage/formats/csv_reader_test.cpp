#include "storage/formats/csv_reader.hpp"

#include <cmath>
#include <sstream>

#include <gtest/gtest.h>

#include "constants.hpp"
#include "storage/backend/mock_storage.hpp"
#include "storage/backend/testdata_storage.hpp"
#include "storage/io_handle/input_handler.hpp"
#include "testing/load_table.hpp"

namespace skyrise {

class CsvReaderTest : public ::testing::Test {
  void SetUp() override {
    mock_storage_ = std::make_shared<MockStorage>();
    testdata_storage_ = std::make_shared<TestdataStorage>();
  }

 protected:
  static std::shared_ptr<TableColumnDefinitions> CreateTableColumnDefinitions() {
    auto table_definitions = std::make_shared<TableColumnDefinitions>();
    table_definitions->emplace_back("L_ORDERKEY", DataType::kLong, false);
    table_definitions->emplace_back("L_PARTKEY", DataType::kLong, false);
    table_definitions->emplace_back("L_SUPPKEY", DataType::kInt, false);
    table_definitions->emplace_back("L_LINENUMBER", DataType::kInt, false);
    table_definitions->emplace_back("L_QUANTITY", DataType::kInt, false);
    table_definitions->emplace_back("L_EXTENDEDPRICE", DataType::kFloat, false);
    table_definitions->emplace_back("L_DISCOUNT", DataType::kDouble, false);
    table_definitions->emplace_back("L_TAX", DataType::kDouble, false);
    table_definitions->emplace_back("L_RETURNFLAG", DataType::kString, false);
    table_definitions->emplace_back("L_LINESTATUS", DataType::kString, false);
    table_definitions->emplace_back("L_SHIPDATE", DataType::kString, false);
    table_definitions->emplace_back("L_COMMITDATE", DataType::kString, false);
    table_definitions->emplace_back("L_RECEIPTDATE", DataType::kString, false);
    table_definitions->emplace_back("L_SHIPINSTRUCT", DataType::kString, false);
    table_definitions->emplace_back("L_SHIPMODE", DataType::kString, false);
    table_definitions->emplace_back("L_COMMENT", DataType::kString, false);
    return table_definitions;
  }

  static inline const std::string kLineItemTblPath = "tbl/tpch_lineitem_top100.tbl";
  static inline const std::string kWellBehavedCsvPath = "csv/well_behaved.csv";
  static inline const std::string kWithTypesCsvPath = "csv/with_types.csv";
  static inline const std::string kOnlyHeaderCsvPath = "csv/only_header.csv";
  static constexpr size_t kBufferSize = 1_KB;
  std::shared_ptr<MockStorage> mock_storage_;
  std::shared_ptr<TestdataStorage> testdata_storage_;
};

TEST_F(CsvReaderTest, TestGoodBehavedExample) {
  const auto csv_reader = BuildFormatReader<CsvFormatReader>(testdata_storage_, kWellBehavedCsvPath);
  auto chunk = csv_reader->Next();

  ASSERT_FALSE(csv_reader->GetError());
  ASSERT_EQ(chunk->Size(), 3);
  ASSERT_EQ(chunk->GetColumnCount(), 2);
}

TEST_F(CsvReaderTest, TypeInference) {
  const auto csv_reader = BuildFormatReader<CsvFormatReader>(testdata_storage_, kWithTypesCsvPath);
  auto chunk = csv_reader->Next();

  ASSERT_FALSE(csv_reader->GetError());
  ASSERT_EQ(chunk->Size(), 1);
  ASSERT_EQ(chunk->GetColumnCount(), 5);

  ASSERT_EQ(std::get<int32_t>((*chunk->GetSegment(0))[0]), 1);
  ASSERT_EQ(std::get<int64_t>((*chunk->GetSegment(1))[0]), 2);
  ASSERT_EQ(std::get<float>((*chunk->GetSegment(2))[0]), 1.2f);
  ASSERT_EQ(std::get<double>((*chunk->GetSegment(3))[0]), 1.23);
  ASSERT_EQ(std::get<std::string>((*chunk->GetSegment(4))[0]), "Hel|o");
}

TEST_F(CsvReaderTest, LineItemContent) {
  auto table_definitions = CreateTableColumnDefinitions();

  CsvFormatReaderOptions configuration;
  configuration.expected_schema = table_definitions;
  configuration.delimiter = '|';
  configuration.guess_delimiter = false;
  configuration.guess_has_header = false;
  configuration.read_buffer_size = kBufferSize;

  const auto csv_reader = BuildFormatReader<CsvFormatReader>(testdata_storage_, kLineItemTblPath, configuration);

  std::unique_ptr<Chunk> next = csv_reader->Next();
  auto first_element = (*next->GetSegment(0))[0];
  auto last_element = (*next->GetSegment(15))[0];

  ASSERT_FALSE(csv_reader->GetError());
  ASSERT_EQ(std::get<int64_t>(first_element), 1);
  ASSERT_EQ(std::get<std::string>(last_element), "egular courts above the");

  ASSERT_FALSE(csv_reader->GetError());
}

TEST_F(CsvReaderTest, LineItemExpectedChunks) {
  auto table_definitions = CreateTableColumnDefinitions();

  CsvFormatReaderOptions configuration;
  configuration.expected_schema = table_definitions;
  configuration.delimiter = '|';
  configuration.guess_delimiter = false;
  configuration.guess_has_header = false;
  configuration.read_buffer_size = kBufferSize;

  const auto csv_reader = BuildFormatReader<CsvFormatReader>(testdata_storage_, kLineItemTblPath, configuration);

  auto object_storage = std::make_shared<TestdataStorage>()->OpenForReading(kLineItemTblPath);
  auto file_size = object_storage->GetStatus().GetSize();

  size_t counter = 0;
  for (counter = 0; csv_reader->HasNext(); ++counter) {
    auto next_chunk = csv_reader->Next();
  }

  const size_t num_at_least_expected_chunks = std::ceil(static_cast<double>(file_size) / kBufferSize);

  ASSERT_GE(counter, num_at_least_expected_chunks);
  std::cout << csv_reader->GetError().GetMessage() << std::endl;
  ASSERT_FALSE(csv_reader->GetError());
}

TEST_F(CsvReaderTest, GuessDelimiter) {
  const std::vector<std::string_view> example_1 = {"id,text", "4,Hello", "6,world", "3,!"};
  EXPECT_EQ(CsvFormatReader::GuessDelimiter(example_1), ',');

  const std::vector<std::string_view> example_2 = {
      "1|2,5|hello",
      "2|3,5|world",
  };

  EXPECT_EQ(CsvFormatReader::GuessDelimiter(example_2), '|');
}

TEST_F(CsvReaderTest, GuessHasHeader) {
  const std::vector<std::vector<std::string_view>> example_1 = {
      {"id", "1", "2", "3"},
      {"name", "person_a", "person_b", "person_c"},
  };
  EXPECT_EQ(CsvFormatReader::GuessHasHeader(example_1), true);

  const std::vector<std::vector<std::string_view>> example_2 = {
      {"id", "one", "two", "three"},
      {"name", "person_a", "person_b", "person_c"},
  };
  EXPECT_EQ(CsvFormatReader::GuessHasHeader(example_2), false);
}

TEST_F(CsvReaderTest, GuessHasTypeInformation) {
  const std::vector<std::vector<std::string_view>> example_1 = {
      {"a", "int", "123458", "123", "123"},
      {"b", "float", "458.7", "456.7", "457.7"},
  };
  EXPECT_EQ(CsvFormatReader::GuessHasTypeInformation(example_1), true);

  const std::vector<std::vector<std::string_view>> example_2 = {
      {"a", "123458", "123", "123"},
      {"b", "458.7", "456.7", "457.7"},
  };
  EXPECT_EQ(CsvFormatReader::GuessHasTypeInformation(example_2), false);
}

TEST_F(CsvReaderTest, WrongSegmentTypeError) {
  auto table_definitions = std::make_shared<TableColumnDefinitions>();
  table_definitions->emplace_back("key", DataType::kLong, false);
  table_definitions->emplace_back("text", DataType::kLong, false);

  CsvFormatReaderOptions configuration;
  configuration.expected_schema = table_definitions;
  configuration.delimiter = ',';
  configuration.guess_delimiter = false;
  configuration.guess_has_header = false;
  configuration.guess_has_types = false;
  configuration.has_header = true;
  configuration.has_types = false;

  const auto csv_reader = BuildFormatReader<CsvFormatReader>(testdata_storage_, kWellBehavedCsvPath, configuration);

  auto chunk = csv_reader->Next();
  EXPECT_TRUE(csv_reader->HasError());
}

TEST_F(CsvReaderTest, BuildSchemaNoHeader) {
  CsvFormatReaderOptions configuration;
  configuration.delimiter = '|';
  configuration.guess_delimiter = false;
  configuration.guess_has_header = false;
  configuration.has_header = false;

  const auto csv_reader = BuildFormatReader<CsvFormatReader>(testdata_storage_, kLineItemTblPath, configuration);

  EXPECT_FALSE(csv_reader->HasError());
  const auto& discovered_schema = csv_reader->GetSchema();
  EXPECT_EQ(discovered_schema->size(), 17);
  for (const auto& column : *discovered_schema) {
    EXPECT_NE(column.name, "");
    EXPECT_EQ(column.data_type, DataType::kString);
  }
}

TEST_F(CsvReaderTest, ReadEmptyFile) {
  const std::string empty_name = "empty";
  auto writer = mock_storage_->OpenForWriting(empty_name);
  writer->Write("", 0);
  writer->Close();

  const auto csv_reader = BuildFormatReader<CsvFormatReader>(mock_storage_, empty_name);

  EXPECT_FALSE(csv_reader->HasError());
  EXPECT_EQ(csv_reader->GetSchema()->size(), 0);
  EXPECT_FALSE(csv_reader->HasNext());
}

TEST_F(CsvReaderTest, ReadFileWithOnlyHeader) {
  CsvFormatReaderOptions configuration;
  configuration.guess_has_header = false;
  configuration.has_header = true;

  const auto csv_reader = BuildFormatReader<CsvFormatReader>(testdata_storage_, kOnlyHeaderCsvPath, configuration);

  EXPECT_FALSE(csv_reader->HasError());
  EXPECT_EQ(csv_reader->GetSchema()->size(), 2);
  EXPECT_FALSE(csv_reader->HasNext());
}

}  // namespace skyrise
