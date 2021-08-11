#include "storage/formats/csv_reader.hpp"

#include <cmath>
#include <sstream>

#include <gtest/gtest.h>

#include "storage/backend/mock_storage.hpp"
#include "storage/backend/testdata_storage.hpp"

namespace skyrise {

class CsvReaderTest : public ::testing::Test {
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
  TestdataStorage storage_;
};

TEST_F(CsvReaderTest, TestGoodBehavedExample) {
  CsvFormatReader csv_reader(storage_.OpenForReading(kWellBehavedCsvPath));
  auto chunk = csv_reader.Next();
  ASSERT_FALSE(csv_reader.GetError());
  ASSERT_EQ(chunk->Size(), 3);
  ASSERT_EQ(chunk->GetColumnCount(), 2);
}

TEST_F(CsvReaderTest, TypeInferenceTest) {
  CsvFormatReader csv_reader(storage_.OpenForReading(kWithTypesCsvPath));
  auto chunk = csv_reader.Next();
  ASSERT_FALSE(csv_reader.GetError());
  ASSERT_EQ(chunk->Size(), 1);
  ASSERT_EQ(chunk->GetColumnCount(), 5);

  ASSERT_EQ(std::get<int32_t>((*chunk->GetSegment(0))[0]), 1);
  ASSERT_EQ(std::get<int64_t>((*chunk->GetSegment(1))[0]), 2);
  ASSERT_EQ(std::get<float>((*chunk->GetSegment(2))[0]), 1.2f);
  ASSERT_EQ(std::get<double>((*chunk->GetSegment(3))[0]), 1.23);
  ASSERT_EQ(std::get<std::string>((*chunk->GetSegment(4))[0]), "Hel|o");
}

TEST_F(CsvReaderTest, LineItemContentTest) {
  auto table_definitions = CreateTableColumnDefinitions();

  CsvFormatReaderOptions configuration;
  configuration.expected_schema = table_definitions;
  configuration.delimiter = '|';
  configuration.guess_delimiter = false;
  configuration.guess_has_header = false;
  configuration.read_buffer_size = kBufferSize;

  auto object_storage = storage_.OpenForReading(kLineItemTblPath);
  CsvFormatReader csv_reader(std::move(object_storage), configuration);

  std::unique_ptr<Chunk> next = csv_reader.Next();
  auto first_element = (*next->GetSegment(0))[0];
  auto last_element = (*next->GetSegment(15))[0];

  ASSERT_FALSE(csv_reader.GetError());
  ASSERT_EQ(std::get<int64_t>(first_element), 1);
  ASSERT_EQ(std::get<std::string>(last_element), "egular courts above the");

  while (csv_reader.HasNext()) {
    auto next_chunk = csv_reader.Next();
    first_element = (*next_chunk->GetSegment(0))[next_chunk->GetSegment(0)->Size() - 1];
    last_element = (*next_chunk->GetSegment(15))[next_chunk->GetSegment(0)->Size() - 1];
  }

  ASSERT_EQ(std::get<int64_t>(first_element), 98);
  ASSERT_EQ(std::get<std::string>(last_element), " cajole furiously. blithely ironic ideas ");
  ASSERT_FALSE(csv_reader.GetError());
}

TEST_F(CsvReaderTest, LineItemExpectedChunksTest) {
  auto table_definitions = CreateTableColumnDefinitions();

  CsvFormatReaderOptions configuration;
  configuration.expected_schema = table_definitions;
  configuration.delimiter = '|';
  configuration.guess_delimiter = false;
  configuration.guess_has_header = false;
  configuration.read_buffer_size = kBufferSize;

  auto object_storage = storage_.OpenForReading(kLineItemTblPath);
  auto file_size = object_storage->GetStatus().GetSize();
  CsvFormatReader csv_reader(std::move(object_storage), configuration);

  size_t counter = 0;
  for (counter = 0; csv_reader.HasNext(); counter++) {
    auto next_chunk = csv_reader.Next();
  }

  size_t num_at_least_expected_chunks = std::ceil(static_cast<double>(file_size) / kBufferSize);

  ASSERT_GE(counter, num_at_least_expected_chunks);
  ASSERT_FALSE(csv_reader.GetError());
}

TEST_F(CsvReaderTest, GuessDelimiterTest) {
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

  CsvFormatReader csv_reader(storage_.OpenForReading(kWellBehavedCsvPath), configuration);
  auto chunk = csv_reader.Next();
  EXPECT_TRUE(csv_reader.HasError());
}

TEST_F(CsvReaderTest, BuildSchemaNoHeaderTest) {
  CsvFormatReaderOptions configuration;
  configuration.delimiter = '|';
  configuration.guess_delimiter = false;
  configuration.guess_has_header = false;
  configuration.has_header = false;

  CsvFormatReader csv_reader(storage_.OpenForReading(kLineItemTblPath), configuration);
  EXPECT_FALSE(csv_reader.HasError());
  const auto& discovered_schema = csv_reader.GetSchema();
  EXPECT_EQ(discovered_schema->size(), 17);
  for (const auto& column : *discovered_schema) {
    EXPECT_NE(column.name, "");
    EXPECT_EQ(column.data_type, DataType::kString);
  }
}

TEST_F(CsvReaderTest, ReadEmptyFile) {
  const std::string empty_name = "empty";
  MockStorage mock_storage;
  auto writer = mock_storage.OpenForWriting(empty_name);
  writer->Write("", 0);
  writer->Close();

  CsvFormatReader csv_reader(mock_storage.OpenForReading(empty_name));
  EXPECT_FALSE(csv_reader.HasError());
  EXPECT_EQ(csv_reader.GetSchema()->size(), 0);
  EXPECT_FALSE(csv_reader.HasNext());
}

TEST_F(CsvReaderTest, ReadFileWithOnlyHeader) {
  CsvFormatReaderOptions configuration;
  configuration.guess_has_header = false;
  configuration.has_header = true;

  CsvFormatReader csv_reader(storage_.OpenForReading(kOnlyHeaderCsvPath), configuration);
  EXPECT_FALSE(csv_reader.HasError());
  EXPECT_EQ(csv_reader.GetSchema()->size(), 2);
  EXPECT_FALSE(csv_reader.HasNext());
}

}  // namespace skyrise
