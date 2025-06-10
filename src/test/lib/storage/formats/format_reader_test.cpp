#include <gtest/gtest.h>

#include "gmock/gmock-matchers.h"
#include "storage/backend/mock_storage.hpp"
#include "storage/backend/test_storage.hpp"
#include "storage/formats/orc_reader.hpp"
#include "storage/formats/orc_writer.hpp"
#include "storage/formats/parquet_reader.hpp"
#include "storage/formats/parquet_writer.hpp"
#include "storage/table/value_segment.hpp"
#include "testing/load_table.hpp"

namespace skyrise {

/**
 * FormatReaderTest contains common base tests for all format readers (e.g., OrcFormatReader and ParquetFormatReader).
 * The test data is located in "resources/test/<format>" and must contain equivalent data for all readers.
 */
template <typename Formatter>
class FormatReaderTest : public ::testing::Test {
 protected:
  void SetUp() override {
    test_storage_ = std::make_shared<TestStorage>();
    mock_storage_ = std::make_shared<MockStorage>();

    if (std::is_same_v<ParquetFormatReader, typename Formatter::first_type::first_type>) {
      object_name_ = "table.parquet";
      file_format_ = "parquet";
      invalid_file_error_message_ =
          "Invalid: Could not open Parquet input source '<Buffer>': Parquet file size is 3 bytes, smaller than the "
          "minimum file footer (8 bytes)";
    } else {
      object_name_ = "table.orc";
      file_format_ = "orc";
      invalid_file_error_message_ = "Could not open ORC input source '<Buffer>': File size too small";
    }
  };

  static std::shared_ptr<Chunk> CreateChunkWithMockData() {
    // Create some dummy data
    auto int_column_of_ones = std::make_shared<ValueSegment<int32_t>>();
    int_column_of_ones->Values().insert(int_column_of_ones->Values().begin(), kChunkDefaultSize, 1);

    auto long_column_of_twos = std::make_shared<ValueSegment<int64_t>>();
    long_column_of_twos->Values().insert(long_column_of_twos->Values().begin(), kChunkDefaultSize, 2);

    auto float_column_of_threes = std::make_shared<ValueSegment<float>>();
    float_column_of_threes->Values().insert(float_column_of_threes->Values().begin(), kChunkDefaultSize, 3.0f);

    auto double_column_of_fours = std::make_shared<ValueSegment<double>>();
    double_column_of_fours->Values().insert(double_column_of_fours->Values().begin(), kChunkDefaultSize, 4.0);

    auto string_column_of_fives = std::make_shared<ValueSegment<std::string>>();
    string_column_of_fives->Values().insert(string_column_of_fives->Values().begin(), kChunkDefaultSize, "Five");

    Segments segments;
    segments.emplace_back(std::move(int_column_of_ones));
    segments.emplace_back(std::move(long_column_of_twos));
    segments.emplace_back(std::move(float_column_of_threes));
    segments.emplace_back(std::move(double_column_of_fours));
    segments.emplace_back(std::move(string_column_of_fives));

    return std::make_shared<Chunk>(segments);
  }

  static TableColumnDefinitions CreateSchemaForChunk() {
    TableColumnDefinitions schema;
    schema.emplace_back("ones", DataType::kInt, false);
    schema.emplace_back("twos", DataType::kLong, false);
    schema.emplace_back("threes", DataType::kFloat, false);
    schema.emplace_back("fours", DataType::kDouble, false);
    schema.emplace_back("fives", DataType::kString, false);

    return schema;
  }

  void WriteMockData() {
    using Writer = typename Formatter::second_type::first_type;
    using Options = typename Formatter::second_type::second_type;
    const Options options;
    auto object_writer = mock_storage_->OpenForWriting(object_name_);
    auto chunk = CreateChunkWithMockData();
    Writer writer(options);
    writer.SetOutputHandler([&object_writer](const char* data, size_t length) { object_writer->Write(data, length); });
    writer.Initialize(CreateSchemaForChunk());

    // Write two chunks
    writer.ProcessChunk(chunk);
    writer.ProcessChunk(chunk);

    writer.Finalize();
    object_writer->Close();
  }

  std::string object_name_;
  std::shared_ptr<MockStorage> mock_storage_;
  std::shared_ptr<TestStorage> test_storage_;
  std::string file_format_;
  std::string invalid_file_error_message_;
};

using FormatterImplementations = ::testing::Types<
    std::pair<std::pair<ParquetFormatReader, ParquetFormatReaderOptions>,
              std::pair<ParquetFormatWriter, ParquetFormatWriterOptions>>,
    std::pair<std::pair<OrcFormatReader, OrcFormatReaderOptions>, std::pair<OrcFormatWriter, OrcFormatWriterOptions>>>;
TYPED_TEST_SUITE(FormatReaderTest, FormatterImplementations, );

TYPED_TEST(FormatReaderTest, WriteAndRead) {
  using Reader = typename TypeParam::first_type::first_type;
  this->WriteMockData();

  const auto reader = BuildFormatReader<Reader>(this->mock_storage_, this->object_name_);

  ASSERT_FALSE(reader->HasError());
  ASSERT_TRUE(reader->HasNext());
  if (this->file_format_ == "parquet") {
    const auto& schema = reader->GetSchema();
    EXPECT_EQ(*schema, this->CreateSchemaForChunk());
  } else {
    // ORC writer does not support writing schema nullability
  }
  {
    auto chunk = reader->Next();
    EXPECT_EQ(chunk->Size(), kChunkDefaultSize);
    EXPECT_EQ(std::get<int32_t>((*chunk->GetSegment(0))[0]), 1);
    EXPECT_EQ(std::get<int64_t>((*chunk->GetSegment(1))[0]), 2L);
    EXPECT_EQ(std::get<float>((*chunk->GetSegment(2))[0]), 3.0f);
    EXPECT_EQ(std::get<double>((*chunk->GetSegment(3))[0]), 4.0);
    EXPECT_EQ(std::get<std::string>((*chunk->GetSegment(4))[0]), "Five");
    EXPECT_EQ(std::get<int32_t>((*chunk->GetSegment(0))[kChunkDefaultSize - 1]), 1);
    EXPECT_EQ(std::get<int64_t>((*chunk->GetSegment(1))[kChunkDefaultSize - 1]), 2L);
    EXPECT_EQ(std::get<float>((*chunk->GetSegment(2))[kChunkDefaultSize - 1]), 3.0f);
    EXPECT_EQ(std::get<double>((*chunk->GetSegment(3))[kChunkDefaultSize - 1]), 4.0);
    EXPECT_EQ(std::get<std::string>((*chunk->GetSegment(4))[kChunkDefaultSize - 1]), "Five");
  }
  EXPECT_TRUE(reader->HasNext());
  {
    auto chunk = reader->Next();
    EXPECT_EQ(chunk->Size(), kChunkDefaultSize);
  }
  EXPECT_FALSE(reader->HasNext());
}

TYPED_TEST(FormatReaderTest, UnexpectedSchema) {
  using Options = typename TypeParam::first_type::second_type;
  using Reader = typename TypeParam::first_type::first_type;
  Options options;
  options.expected_schema = std::make_shared<TableColumnDefinitions>();

  const auto reader = BuildFormatReader<Reader>(
      this->test_storage_, this->file_format_ + "/timestamp_date_bool_varchar." + this->file_format_, options);
  EXPECT_TRUE(reader->HasError());
  EXPECT_EQ(reader->GetError().GetMessage(), "Unexpected schema found.");
}

TYPED_TEST(FormatReaderTest, TypeSupport) {
  using Options = typename TypeParam::first_type::second_type;
  using Reader = typename TypeParam::first_type::first_type;
  Options options;
  options.parse_dates_as_string = true;

  const auto reader = BuildFormatReader<Reader>(
      this->test_storage_, this->file_format_ + "/timestamp_date_bool_varchar." + this->file_format_, options);

  ASSERT_FALSE(reader->HasError());
  ASSERT_TRUE(reader->HasNext());
  auto schema = reader->GetSchema();

  EXPECT_EQ(schema->size(), 4);
  EXPECT_EQ(schema->at(0).name, "a_timestamp");
  EXPECT_EQ(schema->at(0).data_type, DataType::kLong);

  EXPECT_EQ(schema->at(1).name, "a_date");
  EXPECT_EQ(schema->at(1).data_type, DataType::kString);

  EXPECT_EQ(schema->at(2).name, "a_bool");
  EXPECT_EQ(schema->at(2).data_type, DataType::kInt);

  EXPECT_EQ(schema->at(3).name, "a_varchar");
  EXPECT_EQ(schema->at(3).data_type, DataType::kString);

  auto chunk = reader->Next();
  EXPECT_EQ(chunk->Size(), 2);
  EXPECT_FALSE(reader->HasNext());

  EXPECT_TRUE(std::get<int64_t>((*chunk->GetSegment(0))[0]) == 1619535093000000000);

  EXPECT_TRUE(std::get<int64_t>((*chunk->GetSegment(0))[1]) == 1619535093000000000);

  EXPECT_EQ(std::get<std::string>((*chunk->GetSegment(1))[0]), "2021-04-27");
  EXPECT_EQ(std::get<std::string>((*chunk->GetSegment(1))[1]), "2021-04-27");

  EXPECT_EQ(std::get<int32_t>((*chunk->GetSegment(2))[0]), 1);
  EXPECT_EQ(std::get<int32_t>((*chunk->GetSegment(2))[1]), 0);

  EXPECT_EQ(std::get<std::string>((*chunk->GetSegment(3))[0]), "ab");
  EXPECT_EQ(std::get<std::string>((*chunk->GetSegment(3))[1]), "c");
}

TYPED_TEST(FormatReaderTest, DateAsNumericValue) {
  using Reader = typename TypeParam::first_type::first_type;

  const auto reader = BuildFormatReader<Reader>(
      this->test_storage_, this->file_format_ + "/timestamp_date_bool_varchar." + this->file_format_);

  ASSERT_FALSE(reader->HasError());
  ASSERT_TRUE(reader->HasNext());
  auto schema = reader->GetSchema();

  EXPECT_EQ(schema->size(), 4);

  EXPECT_EQ(schema->at(1).name, "a_date");
  EXPECT_EQ(schema->at(1).data_type, DataType::kLong);

  auto chunk = reader->Next();
  EXPECT_EQ(chunk->Size(), 2);
  EXPECT_FALSE(reader->HasNext());

  EXPECT_EQ(std::get<int64_t>((*chunk->GetSegment(1))[0]), 18744);
  EXPECT_EQ(std::get<int64_t>((*chunk->GetSegment(1))[1]), 18744);
}

TYPED_TEST(FormatReaderTest, ReadMoreChunksThanAvailable) {
  using Reader = typename TypeParam::first_type::first_type;

  const auto reader = BuildFormatReader<Reader>(
      this->test_storage_, this->file_format_ + "/timestamp_date_bool_varchar." + this->file_format_);

  while (reader->HasNext()) {
    reader->Next();
  }

  EXPECT_FALSE(reader->HasError());
  auto chunk = reader->Next();
  EXPECT_EQ(chunk, nullptr);
}

TYPED_TEST(FormatReaderTest, ReadInvalidFile) {
  using Reader = typename TypeParam::first_type::first_type;
  const std::string filename = "invalid." + this->file_format_;
  const std::array<char, 3> file_content{0, 1, 2};

  auto writer = this->mock_storage_->OpenForWriting(filename);
  writer->Write(file_content.data(), file_content.size());
  writer->Close();

  const auto reader = BuildFormatReader<Reader>(this->mock_storage_, filename);

  EXPECT_TRUE(reader->HasError());
  EXPECT_THAT(reader->GetError().GetMessage(), ::testing::HasSubstr(this->invalid_file_error_message_));
}

TYPED_TEST(FormatReaderTest, UnsupportedTypes) {
  using Reader = typename TypeParam::first_type::first_type;

  const auto reader =
      BuildFormatReader<Reader>(this->test_storage_, this->file_format_ + "/unsupported_array." + this->file_format_);

  EXPECT_TRUE(reader->HasError());
}

}  // namespace skyrise
