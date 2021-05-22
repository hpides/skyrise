#include "storage/formats/orc_reader.hpp"

#include <gtest/gtest.h>

#include "../backend/mock_storage.hpp"
#include "../backend/testdata_storage.hpp"
#include "storage/formats/orc_writer.hpp"
#include "storage/table/value_segment.hpp"

namespace skyrise {

class OrcFormatReaderTest : public ::testing::Test {
 protected:
  static Chunk CreateChunkWithMockData() {
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

    return Chunk(segments);
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

  void WriteMockOrc() {
    OrcFormatterOptions options;
    auto object_writer = storage_.OpenForWriting(kOrcObjectName);
    Chunk chunk = CreateChunkWithMockData();
    OrcFormatter orc_writer(options);
    orc_writer.SetOutputHandler(
        [&object_writer](const char* data, size_t length) { object_writer->Write(data, length); });
    orc_writer.Initialize(CreateSchemaForChunk());

    // Write two chunks
    orc_writer.ProcessChunk(chunk);
    orc_writer.ProcessChunk(chunk);

    orc_writer.Finalize();
    object_writer->Close();
  }

  std::unique_ptr<ObjectReader> GetOrcObject() { return storage_.OpenForReading(kOrcObjectName); }

  MockStorage storage_;
  TestdataStorage testdata_;
  inline static const std::string kOrcObjectName{"table.orc"};
};

TEST_F(OrcFormatReaderTest, WriteAndRead) {
  WriteMockOrc();
  OrcFormatReader orc_reader(GetOrcObject());
  EXPECT_FALSE(orc_reader.HasError());
  EXPECT_TRUE(orc_reader.HasNext());
  auto schema = orc_reader.GetSchema();
  EXPECT_EQ(*schema, CreateSchemaForChunk());
  {
    auto chunk = orc_reader.Next();
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
  EXPECT_TRUE(orc_reader.HasNext());
  {
    auto chunk = orc_reader.Next();
    EXPECT_EQ(chunk->Size(), kChunkDefaultSize);
  }
  EXPECT_FALSE(orc_reader.HasNext());
}

TEST_F(OrcFormatReaderTest, TypeSupport) {
  OrcFormatReaderOptions options;
  options.parse_dates_as_string = true;
  OrcFormatReader orc_reader(testdata_.OpenForReading("orc/timestamp_date_bool_varchar.orc"), options);
  EXPECT_FALSE(orc_reader.HasError());
  EXPECT_TRUE(orc_reader.HasNext());
  auto schema = orc_reader.GetSchema();

  EXPECT_EQ(schema->size(), 4);
  EXPECT_EQ(schema->at(0).name, "a_timestamp");
  EXPECT_EQ(schema->at(0).data_type, DataType::kLong);

  EXPECT_EQ(schema->at(1).name, "a_date");
  EXPECT_EQ(schema->at(1).data_type, DataType::kString);

  EXPECT_EQ(schema->at(2).name, "a_bool");
  EXPECT_EQ(schema->at(2).data_type, DataType::kInt);

  EXPECT_EQ(schema->at(3).name, "a_varchar");
  EXPECT_EQ(schema->at(3).data_type, DataType::kString);

  auto chunk = orc_reader.Next();
  EXPECT_EQ(chunk->Size(), 2);
  EXPECT_FALSE(orc_reader.HasNext());

  EXPECT_EQ(std::get<int64_t>((*chunk->GetSegment(0))[0]), 1619535093);
  EXPECT_EQ(std::get<int64_t>((*chunk->GetSegment(0))[1]), 1619535093);

  EXPECT_EQ(std::get<std::string>((*chunk->GetSegment(1))[0]), "2021-04-27");
  EXPECT_EQ(std::get<std::string>((*chunk->GetSegment(1))[1]), "2021-04-27");

  EXPECT_EQ(std::get<int32_t>((*chunk->GetSegment(2))[0]), 1);
  EXPECT_EQ(std::get<int32_t>((*chunk->GetSegment(2))[1]), 0);

  EXPECT_EQ(std::get<std::string>((*chunk->GetSegment(3))[0]), "ab");
  EXPECT_EQ(std::get<std::string>((*chunk->GetSegment(3))[1]), "c");
}

TEST_F(OrcFormatReaderTest, DateAsNumericValue) {
  OrcFormatReader orc_reader(testdata_.OpenForReading("orc/timestamp_date_bool_varchar.orc"));
  EXPECT_FALSE(orc_reader.HasError());
  EXPECT_TRUE(orc_reader.HasNext());
  auto schema = orc_reader.GetSchema();

  EXPECT_EQ(schema->size(), 4);

  EXPECT_EQ(schema->at(1).name, "a_date");
  EXPECT_EQ(schema->at(1).data_type, DataType::kLong);

  auto chunk = orc_reader.Next();
  EXPECT_EQ(chunk->Size(), 2);
  EXPECT_FALSE(orc_reader.HasNext());

  EXPECT_EQ(std::get<int64_t>((*chunk->GetSegment(1))[0]), 18744);
  EXPECT_EQ(std::get<int64_t>((*chunk->GetSegment(1))[1]), 18744);
}

TEST_F(OrcFormatReaderTest, UnexpectedSchema) {
  OrcFormatReaderOptions options;
  options.expected_schema = std::make_shared<TableColumnDefinitions>();

  OrcFormatReader orc_reader(testdata_.OpenForReading("orc/timestamp_date_bool_varchar.orc"), options);
  EXPECT_TRUE(orc_reader.HasError());
  EXPECT_EQ(orc_reader.GetError().GetMessage(), "Unexpected schema found.");
}

TEST_F(OrcFormatReaderTest, UnsupportedTypes) {
  OrcFormatReader orc_reader(testdata_.OpenForReading("orc/unsupported_array.orc"));
  EXPECT_TRUE(orc_reader.HasError());
}

TEST_F(OrcFormatReaderTest, ReadMoreChunksThanAvailable) {
  OrcFormatReader orc_reader(testdata_.OpenForReading("orc/timestamp_date_bool_varchar.orc"));

  while (orc_reader.HasNext()) {
    orc_reader.Next();
  }

  EXPECT_FALSE(orc_reader.HasError());
  auto chunk = orc_reader.Next();
  EXPECT_EQ(chunk, nullptr);
}

TEST_F(OrcFormatReaderTest, ReadInvalidFile) {
  const std::string filename = "invalid.orc";
  const std::array<char, 3> file_content{0, 1, 2};

  auto writer = storage_.OpenForWriting(filename);
  writer->Write(file_content.data(), file_content.size());
  writer->Close();

  OrcFormatReader orc_reader(storage_.OpenForReading(filename));
  EXPECT_TRUE(orc_reader.HasError());
  EXPECT_EQ(orc_reader.GetError().GetMessage(), "File size too small");
}

}  // namespace skyrise
