#include <gtest/gtest.h>

#include "storage/backend/mock_storage.hpp"
#include "storage/formats/abstract_chunk_reader.hpp"
#include "storage/formats/csv_reader.hpp"
#include "storage/formats/mock_chunk_reader.hpp"
#include "storage/formats/orc_reader.hpp"
#include "storage/formats/parquet_reader.hpp"
#include "storage/io_handle/mock_object_buffer.hpp"
#include "storage/table/value_segment.hpp"

namespace skyrise {

class ChunkReaderTest : public ::testing::Test {
  using ValueSegmentGenerators = std::vector<ValueSegmentGenerator>;

  void SetUp() override {
    ValueSegmentGenerators segment_generators;

    const auto string_segment_generator_callback = []() {
      return std::make_shared<ValueSegment<std::string>>(std::vector<std::string>(kSizeSegments, "Test"));
    };

    const auto int_segment_generator_callback = []() {
      return std::make_shared<ValueSegment<int32_t>>(std::vector<int32_t>(kSizeSegments, 5));
    };

    const auto float_segment_generator_callback = []() {
      return std::make_shared<ValueSegment<float>>(std::vector<float>(kSizeSegments, 5.0f));
    };

    segment_generators.emplace_back(string_segment_generator_callback);
    segment_generators.emplace_back(int_segment_generator_callback);
    segment_generators.emplace_back(float_segment_generator_callback);

    auto schema = std::make_shared<TableColumnDefinitions>();
    schema->emplace_back("mock_schema", DataType::kString, false);
    schema->emplace_back("mock_schema_two", DataType::kFloat, false);

    mock_formatter_configuration_.num_chunks = kNumChunksMockFormatter;
    mock_formatter_configuration_.generators = segment_generators;
    mock_formatter_configuration_.schema = schema;

    mock_formatter_configuration_error_.num_chunks = kNumChunksMockFormatter;
    mock_formatter_configuration_error_.generators = segment_generators;
    mock_formatter_configuration_error_.error = true;
    mock_formatter_configuration_error_.num_chunks_until_error = kNumChunksUntilErrorMockFormatter;

    mock_formatter_configuration_initialization_error_.num_chunks = kNumChunksMockFormatter;
    mock_formatter_configuration_initialization_error_.generators = segment_generators;
    mock_formatter_configuration_initialization_error_.error = true;
    mock_formatter_configuration_initialization_error_.num_chunks_until_error = 0;

    orc_options_.parse_dates_as_string = true;

    csv_options_.has_header = true;
    csv_options_.has_types = true;

    mock_storage_ = std::make_shared<MockStorage>();
  }

 protected:
  static constexpr size_t kNumSequentialTasks = 3;
  static constexpr size_t kNumChunksMockFormatter = 3;
  static constexpr size_t kNumChunksUntilErrorMockFormatter = 2;
  static constexpr size_t kSizeSegments = 100;

  CsvFormatReaderOptions csv_options_;
  OrcFormatReaderOptions orc_options_;
  ParquetFormatReaderOptions parquet_options_;

  MockChunkReaderConfiguration mock_formatter_configuration_;
  MockChunkReaderConfiguration mock_formatter_configuration_error_;
  MockChunkReaderConfiguration mock_formatter_configuration_initialization_error_;
  std::shared_ptr<MockStorage> mock_storage_;
};

TEST_F(ChunkReaderTest, GetFormatReaderFactoryWithDefaultConfiguration) {
  // This tests, if the following code compiles.
  [[maybe_unused]] auto factory = std::make_shared<FormatReaderFactory<MockChunkReader>>();
}

TEST_F(ChunkReaderTest, ChunkErrorTest) {
  auto format_factory = std::make_shared<FormatReaderFactory<MockChunkReader>>(mock_formatter_configuration_error_);
  const std::shared_ptr<skyrise::AbstractChunkReader> reader = format_factory->Get(nullptr, 0);

  size_t counter_chunk = 0;
  for (; reader->HasNext(); ++counter_chunk) {
    auto chunk = reader->Next();
  }
  EXPECT_TRUE(reader->HasError());
  EXPECT_EQ(counter_chunk, kNumChunksUntilErrorMockFormatter);
}

TEST_F(ChunkReaderTest, InitializationErrorTest) {
  auto format_factory =
      std::make_shared<FormatReaderFactory<MockChunkReader>>(mock_formatter_configuration_initialization_error_);
  const std::shared_ptr<skyrise::AbstractChunkReader> reader = format_factory->Get(nullptr, 0);

  size_t counter_chunk = 0;
  for (; reader->HasNext(); ++counter_chunk) {
    auto chunk = reader->Next();
  }
  EXPECT_TRUE(reader->HasError());
  EXPECT_EQ(counter_chunk, 0);
}

TEST_F(ChunkReaderTest, OrcFormatterTest) {
  auto mock_object_buffer = std::make_shared<MockObjectBuffer>("orc/with_types.orc");
  auto factory = std::make_shared<FormatReaderFactory<OrcFormatReader>>(orc_options_);

  const std::shared_ptr<skyrise::AbstractChunkReader> reader =
      factory->Get(mock_object_buffer, mock_object_buffer->BufferSize());

  EXPECT_FALSE(reader->HasError());
  EXPECT_TRUE(reader->HasNext());

  size_t chunk_counter = 0;
  while (reader->HasNext()) {
    auto chunk = reader->Next();
    if (chunk != nullptr) {
      ++chunk_counter;
    }
  }

  ASSERT_FALSE(reader->HasError());
  ASSERT_EQ(chunk_counter, 1);
}

TEST_F(ChunkReaderTest, ParquetFormatterTest) {
  auto mock_object_buffer = std::make_shared<MockObjectBuffer>("parquet/with_types.parquet");
  auto factory = std::make_shared<FormatReaderFactory<ParquetFormatReader>>(parquet_options_);
  const std::shared_ptr<skyrise::AbstractChunkReader> reader =
      factory->Get(mock_object_buffer, mock_object_buffer->BufferSize());

  EXPECT_FALSE(reader->HasError());
  EXPECT_TRUE(reader->HasNext());

  size_t chunk_counter = 0;
  while (reader->HasNext()) {
    auto chunk = reader->Next();
    if (chunk != nullptr) {
      ++chunk_counter;
    }
  }

  ASSERT_FALSE(reader->HasError());
  ASSERT_EQ(chunk_counter, 1);
}

TEST_F(ChunkReaderTest, CsvFormatterTest) {
  auto mock_object_buffer = std::make_shared<MockObjectBuffer>("csv/with_types.csv");
  auto factory = std::make_shared<FormatReaderFactory<CsvFormatReader>>(csv_options_);
  const std::shared_ptr<skyrise::AbstractChunkReader> reader =
      factory->Get(mock_object_buffer, mock_object_buffer->BufferSize());

  EXPECT_FALSE(reader->HasError());
  EXPECT_TRUE(reader->HasNext());

  size_t chunk_counter = 0;
  while (reader->HasNext()) {
    auto chunk = reader->Next();
    if (chunk != nullptr) {
      ++chunk_counter;
    }
  }

  ASSERT_FALSE(reader->HasError());
  ASSERT_EQ(chunk_counter, 1);
}

TEST_F(ChunkReaderTest, DiscoverSchemaTest) {
  const auto format_factory = std::make_shared<FormatReaderFactory<MockChunkReader>>(mock_formatter_configuration_);
  const std::shared_ptr<skyrise::AbstractChunkReader> format_reader = format_factory->Get(nullptr, 0);

  EXPECT_FALSE(format_reader->HasError());
  EXPECT_TRUE(format_reader->HasNext());

  auto schema = format_reader->GetSchema();
  ASSERT_EQ(schema->size(), 2);

  while (format_reader->HasNext()) {
    format_reader->Next();
  }

  EXPECT_FALSE(format_reader->HasError());
}

TEST_F(ChunkReaderTest, DefaultConfigurationTest) {
  auto format_factory = std::make_shared<FormatReaderFactory<MockChunkReader>>();
  const std::shared_ptr<skyrise::AbstractChunkReader> reader = format_factory->Get(nullptr, 0);

  EXPECT_FALSE(reader->HasError());
  EXPECT_TRUE(reader->HasNext());

  size_t chunk_counter = 0;
  while (reader->HasNext()) {
    auto chunk = reader->Next();
    if (chunk != nullptr) {
      ++chunk_counter;
    }
  }

  EXPECT_EQ(chunk_counter, kNumChunksMockFormatter);
  EXPECT_FALSE(reader->HasError());
}

}  // namespace skyrise
